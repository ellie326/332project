package worker

import scala.concurrent.{ExecutionContext, Future}
import scala.annotation.tailrec

import worker.WorkerGrpc
import worker.{DistributeStartRequest, DistributeStartResponse, SortStartRequest, SortStartResponse}

import io.grpc.{Server, ServerBuilder}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import java.net.InetAddress



object Worker extends App {
  private val logger: Logger = Logger("Worker")
  private val arguments: Map[String, List[String]] = CheckAndParseArgument(args.toList)

  private val inputDirectories: List[String] = arguments("-I")
  private val outputDirectory: String = arguments("-O")

  private val dataProcessor : DataProcess = new DataProcess(inputDirectories,outputDirectory)

  private val ip: String = "127.0.0.1"
  private val port: Int = 60051
  private val masterIp: String = "141.223.16.227"
  private val masterPort: Int = 50051

  private def CheckAndParseArgument(args: List[String]): Map[String, List[String]] = {
    if (args.isEmpty) {
      logger.warn(s"Too few arguments: ${args.length}")
      sys.exit(1)
    }

    val parsedArgs = ParseArgument(Map(), "", args)
    if (!parsedArgs.contains("-O") || parsedArgs("-O").length != 1) {
      logger.warn(s"Invalid arguments: $parsedArgs")
      sys.exit(1)
    }

    parsedArgs
  }


  @tailrec
  private def ParseArgument(map: Map[String, List[String]], previousOption: String, args: List[String]): Map[String, List[String]] = {
    args match {
      case Nil => map

      case "-I" :: Remainder =>
        ParseArgument(map, "-I", Remainder)

      case "-O" :: Remainder =>
        ParseArgument(map, "-O", Remainder)

      case value :: Remainder if previousOption.nonEmpty =>

        val updatedMap = map + (previousOption -> value)

        ParseArgument(updatedMap, "", Remainder)

      case value :: Remainder if previousOption.isEmpty =>
      
        val updatedMap = map + ("UNSPECIFIED" -> (map.getOrElse("UNSPECIFIED", List()) :+ value))
        ParseArgument(updatedMap, "", Remainder)
    }
  }


  private val masterChannel: ManagedChannel = {
    ManagedChannelBuilder.forAddress(masterIp,masterport)
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build
  }

  
  // Get Samples 
  private def sendRegister: Future[Unit] = async {
    val samples: List[Record] = await(this.samples)
    val channel: ManagedChannel = channel
    val stub: MasterGrpc.MasterStub = MasterGrpc.stub(channel)
    val request: RegisterRequest = RegisterRequest(ip = NetworkConfig.ip, samples = samples)
    val response: Future[RegisterResponse] = stub.register(request)
    try {
      val responseIp = await(response).ip
      if (responseIp != masterIp) {
        logger.error("response IP is not a masterIp")
        throw new IllegalStateException("response IP is not a masterIp")
      }
      logger.info(s"Sent Register Request to Master at $masterIp:${NetworkConfig.port}")
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to send Register Request: ${ex.getMessage}")
        throw ex
    }
    ()
  }

  private val samples: Future[List[Record]] = DataProcess.getSamplesFromUnsorted
  sendRegister 

  // Shuffling 
  private def prepareRanges(): Future[(SortedMap[String, ByteString], List[String], List[ByteString])] = async {
    val ranges = await(ShuffleStartComplete.future)
    val workerIps = ranges.keys.toList
    val rangeBegins = ranges.values.toList
    (ranges, workerIps, rangeBegins)
  }

  private def prepareData(): Future[(List[BufferedSource], List[Iterator[Record]])] = async {
    val (bufferedSourceList, iteratorList) = await(DataProcess.getDataFromInputSorted).unzip
    (bufferedSourceList, iteratorList)
  }

  private def prepareGrpcStubs(workerIps: List[String]): (List[ManagedChannel], List[WorkerGrpc.WorkerBlockingStub]) = {
    val workerChannels = workerIps.map { ip =>
      ManagedChannelBuilder.forAddress(ip, NetworkConfig.port)
        .usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    }
    val workerBlockingStubs = workerChannels.map(WorkerGrpc.blockingStub)
    (workerChannels, workerBlockingStubs)
  }

  private def distributeRecords(
      iteratorList: List[Iterator[Record]],
      rangeBegins: List[ByteString],
      workerBlockingStubs: List[WorkerGrpc.WorkerBlockingStub]
  ): Unit = {
    @tailrec
    def sendShuffleData(records: List[Record]): Unit = {
      if (records.isEmpty) ()
      else {
        val designatedWorker = getDesignatedWorker(records.head, rangeBegins)
        val (dataToSend, remainder) = records.span(record => designatedWorker == getDesignatedWorker(record, rangeBegins))

        val blockingStub = workerBlockingStubs(designatedWorker)
        val request = ShuffleRequest(ip = NetworkConfig.ip, records = dataToSend)
        blockingStub.distribute(request)

        sendShuffleData(remainder)
      }
    }

    blocking {
      iteratorList.foreach { iter => sendShuffleData(iter.toList) }
    }
  }

  private def getDesignatedWorker(record: Record, rangeBegins: List[ByteString]): Int = {
    val recordKey = record.key
    val blockingStubIdx = rangeBegins.lastIndexWhere(rangeBegin => recordKey >= rangeBegin)
    if (blockingStubIdx == -1) 0 else blockingStubIdx
  }

  private def closeBufferedSources(bufferedSourceList: List[BufferedSource]): Unit = {
    bufferedSourceList.foreach(DataProcess.closeRecordsToDistribute)
  }

  private def shutdownChannels(workerChannels: List[ManagedChannel]): Unit = {
    workerChannels.foreach(_.shutdown)
  }

  private def ShuffleData: Future[Unit] = async {
    val (ranges, workerIps, rangeBegins) = await(prepareRanges())
    val (bufferedSourceList, iteratorList) = await(prepareData())
    val (workerChannels, workerBlockingStubs) = prepareGrpcStubs(workerIps)

    try {
      logger.info(s"Shuffle Starts")
      distributeRecords(iteratorList, rangeBegins, workerBlockingStubs)
      shutdownChannels(workerChannels)
    } finally {
      closeBufferedSources(bufferedSourceList)
    }

    logger.info(s"send shuffle request to designated workers")
    ()
  }


  private def sendShuffleCompleteToMaster: Future[Unit] = async {
    await(ShuffleComplete)

    val masterChannel: ManagedChannel = channel
    val MasterStub: MasterGrpc.MasterStub = MasterGrpc.stub(masterChannel)
    val request: ShuffleCompleteRequest = ShuffleCompleteRequest(ip = NetworkConfig.ip)
    val response: Future[ShuffleCompleteResponse] = stub.ShuffleComplete(request)

    logger.info(s"Sent shuffle complete request to master")
    ()
  }

  private val ShuffleStartComplete: Promise[SortedMap[String, ByteString]] = Promise()
  private val ShuffleComplete: Future[Unit] = ShuffleData
  sendShuffleCompleteToMaster
  
  // Merge Sort 

  private def MergeSort: Future[Unit] = async {
    await(MergeSortStartComplete.future)
    logger.info(s"Start Merge Sort")
    DataProcess.MergeSortandSave()
  }

  private def sendMergeSortCompleteToMaster: Future[Unit] = async {
    await(MergeSortComplete)
    val masterChannel: ManagedChannel = channel
    val masterStub: MasterGrpc.MasterStub = MasterGrpc.stub(masterChannel)
    val request: SortCompleteRequest = SortCompleteRequest(ip = NetworkConfig.ip)
    val response: Future[SortCompleteResponse] = masterStub.sortComplete(request)

    logger.info(s"Merge Sort complete")
    await(response)
    logger.info(s"Sent Merge Sort Complete Request to master")
    ()
  }

  private val MergeSortStartComplete: Promise[Unit] = Promise()
  private val MergeSortComplete: Future[Unit] = MergeSort
  private val ShuffleAndMergeComplete: Future[Unit] = sendMergeSortCompleteToMaster

  /* All the code above executes asynchronously.
   * As as result, this part of code is reached immediately.
   */
  logger.info(s"Server started at ${NetworkConfig.ip}:${NetworkConfig.port}")
  blocking {
    Await.result(ShuffleAndMergeComplete, Duration.Inf)
  }

  // 종료 시 서버 클린업
  sys.addShutdownHook {
    println("Shutting down worker...")
    server.shutdown()
  }

  server.awaitTermination()

  // Worker 서비스 구현
  private class WorkerImpl extends WorkerGrpc.Worker {
    override def StartShuffle(request: ShuffleStartRequest): Future[ShuffleStartResponse] = {
      logger.info(s"Start Shuffle with range: ${request.ranges}")
      ShuffleStartComplete success request.ranges.to(SortedMap)
      Future(ShuffleStartResponse())
    }
    override def MergeSortStart(request: MergeSortStartRequest): Future[MergeSortStartResponse] = {
      logger.info(s"Received Merge Sort Start Request")
      MergeSortStartComplete success ()
      Future(MergeSortStartComplete())
    }
    
  }
  
}
