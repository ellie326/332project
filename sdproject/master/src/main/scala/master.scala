import io.grpc.{Server, ServerBuilder, ManagedChannel, ManagedChannelBuilder}
import master.MasterGrpc
import worker.WorkerGrpc
import master.{RegisterRequest, RegisterResponse, ShuffleCompleteRequest, ShuffleCompleteResponse, MergeSortCompleteRequest, MergeSortCompleteResponse}
import worker.{ShuffleStartRequest, ShuffleStartResponse, MergeSortStartRequest, MergeSortStartResponse}
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.{Promise, Future, ExecutionContext, Await}
import scala.concurrent.duration.Duration
import scala.concurrent.blocking
import com.google.protobuf.ByteString
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import java.net.InetAddress



object Master extends App {
  private val ip: String = "141.223.16.227"
  private val port: Int = 7777
  private val workerNum: Int = args(0).toInt
  private val registerRequests: ConcurrentLinkedQueue[RegisterRequest] = new ConcurrentLinkedQueue[RegisterRequest]()

  private val CompleteAllRegister : Promise[Unit] = Promise()

  private val workerData: Future[(List[String], List[ByteString])] = getWorkerData

  private val workerIps: Future[List[String]] = workerData.map(_._1)
  private val ranges: Future[List[ByteString]] = workerData.map(_._2)

  private val workerChannels : Future[List[ManagedChannel]] = async{
    val ip_ = await(workerIps)

    def createChannel(ip: String): ManagedChannel = {
      ManagedChannelBuilder.forAddress(ip,port).
        usePlaintext().
        asInstanceOf[ManagedChannelBuilder[_]].
        build()
    }

    val channels = ips.map(createChannel)

    channels
  }

  sendShuffleStart

  private val shuffleCompleteRequests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
  private val CompleteAllShuffle: Promise[Unit] = Promise()
  private val shuffleCompletedIp: Future[List[String]] =  getShuffleCompletedIps

  sendMergesortStart

  private val sortCompleteRequests: ConcurrentLinkedQueue[MergeSortCompleteRequest] = new ConcurrentLinkedQueue[MergeSortCompleteRequest]()
  private val CompleteAllMergesort: Promise[Unit] = Promise()


  
  


  // gRPC 서버 설정 및 시작
  private val server = ServerBuilder
    .forPort(port)
    .addService(MasterGrpc.bindService(new MasterImpl, ExecutionContext.global))  // 생성된 서비스 추가
    .build
    .start

  blocking{
    Await.result(CompleteAllMergesort.future, Duration.Inf)
  }
  blocking{
    Await.result(workerChannels,Duration.Inf).foreach(_.shutdown())
  }
  server.shutdown()
  private val result: List[MergeSortCompleteRequest]= sortCompleteRequests.asScala.toList




  // gRPC 서비스 구현
  private class MasterImpl extends MasterGrpc.Master {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      logRequest(request)
      saveRequest(request)
      isComplete()
      buildResponse()
    }

    private def logRequest(request:RegisterRequest): Unit = {
      logger.info(s"Register received request from ${request.ip}")
    }
    
    private def saveRequest(request:RegisterRequest): Unit = {
      registerRequests.add(request)
    }

    private def isComplete(): Unit = {
      if(registerRequests.size == workerNum){
        CompleteAllRegister.trySuccess(())
      }
    }

    private def buildResponse():Unit={
      Future.successful(RegisterResponse(ip=ip))
    }

    override def shuffleComplete(request: ShuffleCompleteRequest): Future[ShuffleCompleteResponse] = {
      logger.info(s"Distribute recieved complete request from ${request.ip}")
      shuffleCompleteRequests.add(request.ip)
      if(shuffleCompleteRequests.size == workerNum){
        CompleteAllShuffle trySuccess()
      }
      Future(ShuffleCompleteResponse())
    }

    override def mergeSortComplete(request: MergeSortCompleteRequest): Future[MergeSortCompleteResponse] = {
      sortCompleteRequests.add(request)
      if(sortCompleteRequests.size == workerNum){
        CompleteAllMergesort trySuccess()
      }
      Future(MergeSortCompleteResponse())
    }
  }

  private def getWorkerData: Future[(List[String],List[ByteString])] = async{
    await(CompleteAllRegister.future)

    val SortedRequests = registerRequests.asScala.toList.sortBy(_.ip)

    val workerIps = SortedRequests.map(_.ip)

    val keys = SortedRequests.flatMap(_.samples.map(_.key))

    val rangeSize = math.ceil(keys.length.toDouble/workerNum).toInt
    val ranges = keys.sorted.grouped(rangeSize).map(_.head).toList

    (workerIps,ranges)
  }

  private def sendShuffleStart: Future[Unit] = async{
    val workerIps = await(this.workerIps)
    val ranges = await(this.ranges)
    val channels = await(this.workerChannels)
    val IpRange : Map[String, ByteString] = (workerIps zip ranges).toMap
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val requests: ShuffleStartRequest = ShuffleStartRequest(ranges = IpRange)
    val responses: List[ShuffleStartResponse] = blockingStubs map(_.StartShuffle(request)) 
    ()
  }

  private def getShuffleCompletedIps: Future[List[String]] =async {
    await(CompleteAllShuffle.future)
    val ips = shuffleCompleteRequests.asScala.toList.sorted
    ips
  }

  private def sendMergesortStart = async {
    val workerIps = await(shuffleCompletedIp)
    val channels = await(workerChannels)
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val request: MergeSortStartRequest = MergeSortStartRequest()
    val responses: List[MergeSortStartResponse] = blockingStubs map(_.MergeSortStart(request))
    ()
  }
}
