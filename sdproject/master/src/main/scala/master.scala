
package com.orange.master
import io.grpc.{Server, ServerBuilder, ManagedChannel, ManagedChannelBuilder}

import com.orange.proto.register._
import com.orange.proto.data._
import com.orange.proto.shuffle._
import com.orange.proto.sort._
import com.orange.proto.master._
import com.orange.proto.worker._
import java.net.NetworkInterface
import scala.sys.process._


import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.{Promise, Future, ExecutionContext, Await}
import com.typesafe.scalalogging.Logger
import scala.concurrent.duration.Duration
import scala.async.Async._
import scala.concurrent.blocking
import com.google.protobuf.ByteString
import com.orange.bytestring_ordering.ByteStringOrdering._

import scala.math.Ordered.orderingToOrdered

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._
import java.net.InetAddress
import com.orange.network.NetworkConfig



object Master extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  private val logger: Logger = Logger("Master")
  private val ip: String = NetworkConfig.ip
  private val masterPort: Int = 50051
  private val workerNum: Int = args(0).toInt

  // Master 실행
  println(s"Master is running at ${NetworkConfig.ip}:$masterPort and waiting for $workerNum workers...")

  // Worker 등록 상태 관리
  private val registerRequests: ConcurrentLinkedQueue[RegisterRequest] = new ConcurrentLinkedQueue[RegisterRequest]()
  private val CompleteAllRegister : Promise[Unit] = Promise()

  logger.info(s"worker data starts")
  private val workerData: Future[(List[String], List[ByteString])] = getWorkerData


  workerData
    .map { case (strings, byteStrings) =>
      // 성공 시 데이터 처리
      logger.info(s"worker Data received. Strings: $strings, ByteStrings: $byteStrings")
    }
    .recover { case exception =>
      // 실패 시 예외 처리
      logger.error("Failed to get worker data, shutting down server.", exception)
      shutdownServer("Error occurred while retrieving worker data.")
    }

  // 샘플 받아서 정렬 후 피봇팅 처리
  private val workerIps: Future[List[String]] = workerData.map(_._1)
  private val ranges: Future[List[ByteString]] = workerData.map(_._2)
  private val workerChannels : Future[List[ManagedChannel]] = async{
    val ips = await(workerIps)

    def createChannel(is: String): ManagedChannel = {
      ManagedChannelBuilder.forAddress(is,50052).
        usePlaintext().
        asInstanceOf[ManagedChannelBuilder[_]].
        build()
    }
    val channels = ips.map(createChannel)
    channels
  }

  logger.info(s"Project start")

  // Shuffle
  sendShuffleStart
  private val shuffleCompleteRequests: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]()
  private val CompleteAllShuffle: Promise[Unit] = Promise()
  private val shuffleCompletedIp: Future[List[String]] =  getShuffleCompletedIps

  // Merge
  sendMergesortStart
  private val sortCompleteRequests: ConcurrentLinkedQueue[MergeSortCompleteRequest] = new ConcurrentLinkedQueue[MergeSortCompleteRequest]()
  private val CompleteAllMergesort: Promise[Unit] = Promise()

  blocking{
    Await.result(CompleteAllMergesort.future, Duration.Inf)
    logger.info(s"All Merge Sort Complete!")
  }

  blocking{
    Await.result(workerChannels,Duration.Inf).foreach(_.shutdown())
  }
  server.shutdown()

  private val result: List[MergeSortCompleteRequest]= sortCompleteRequests.asScala.toList

  // Worker 등록 요청 처리
  private def getWorkerData: Future[(List[String],List[ByteString])] = async{
    logger.info(s"wait for register starts")
    await(CompleteAllRegister.future)

    val listRequests = registerRequests.asScala.toList
    val workerIps = listRequests.map(_.ip).sorted
    logger.info(s"Received all register requests, worker ips: $workerIps")

    val keys = for{
      request <- listRequests
      sample <- request.samples
    }yield sample.key

    val rangeSize = math.ceil(keys.length.toDouble/workerNum).toInt
    val ranges = keys.sorted.grouped(rangeSize).map(_.head).toList
    logger.info(s"Received register ip, range: $workerIps, $ranges")
    (workerIps,ranges)
  }

  // Shuffle 시작
  private def sendShuffleStart: Future[Unit] = async{
    val workerIps = await(this.workerIps)
    val ranges = await(this.ranges)
    val channels = await(this.workerChannels)
    val IpRange : Map[String, ByteString] = (workerIps zip ranges).toMap
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val request: ShuffleStartRequest = ShuffleStartRequest(ranges = IpRange)
    val responses: List[ShuffleStartResponse] = blockingStubs.map(_.startShuffle(request))

    logger.info(s"Send Shuffle start to workers")
    ()
  }

  // Shuffle 완료 IP 수집
  private def getShuffleCompletedIps: Future[List[String]] =async {
    await(CompleteAllShuffle.future)
    val ips = shuffleCompleteRequests.asScala.toList.sorted
    logger.info(s"Received all Shuffle complete $ips")
    ips
  }

  // Merge 시작
  private def sendMergesortStart = async {
    val workerIps = await(shuffleCompletedIp)
    val channels = await(workerChannels)
    val blockingStubs: List[WorkerGrpc.WorkerBlockingStub] = channels map WorkerGrpc.blockingStub
    val request: MergeSortStartRequest = MergeSortStartRequest()
    val responses: List[MergeSortStartResponse] = blockingStubs map(_.mergeSortStart(request))
    logger.info(s"Send Merge Sort Start to workers")
    ()
  }

  // 서버 종료 로직
  private def shutdownServer(message: String): Unit = {
    logger.info(s"Server shutting down: $message")
    System.exit(1)
  }

  // gRPC 서버 시작
  private val server = ServerBuilder
    .forPort(masterPort)
    .addService(MasterGrpc.bindService(new MasterImpl, ExecutionContext.global))  // 생성된 서비스 추가
    .build
    .start

  logger.info(s"Server started at ${ip}:50051")

  // gRPC 서비스 구현
  private class MasterImpl extends MasterGrpc.Master {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      logger.info(s"Register received complete request from ${request.ip}")
      registerRequests.add(request)
      logger.info(s"Current registered workers: ${registerRequests.size}")
      if (registerRequests.size == workerNum) {
        CompleteAllRegister.trySuccess(())
      }
      Future.successful(RegisterResponse(ip = ip))
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
      logger.info(s"Received Merge Sort Complete Request from ${request.ip}")
      sortCompleteRequests.add(request)
      if(sortCompleteRequests.size == workerNum){
        CompleteAllMergesort trySuccess()
      }
      Future(MergeSortCompleteResponse())
    }
  }

}
