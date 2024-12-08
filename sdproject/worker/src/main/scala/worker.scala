package com.orange.worker

import com.orange.proto.register._
import com.orange.proto.data._
import com.orange.proto.shuffle._
import com.orange.proto.sort._
import com.orange.proto.master._
import com.orange.proto.worker._

import io.grpc.{Server, ServerBuilder}


import com.typesafe.scalalogging.Logger
import scala.concurrent.{Future, Promise, Await, ExecutionContext, blocking}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.annotation.tailrec
import scala.async.Async.{async, await}
import scala.collection.SortedMap
import scala.io.BufferedSource
import com.google.protobuf.ByteString
import com.orange.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

import com.orange.network.NetworkConfig


object Worker extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  private val logger: Logger = Logger("Worker")
  logger.info(s"Start parsing the argument")
  private val arguments: Map[String, List[String]] = CheckAndParseArgument(args.toList)
  private val inputDirectories: List[String] = arguments("-I")
  private val outputDirectory: String = arguments("-O").head
  private val masterIp: String = arguments("masterip").head
  private val masterPort: String = arguments("masterport").head
  logger.info(s"masterIP : $masterIp, masterPort : $masterPort")
  logger.info(s"argumnents parsed")


  private val dataProcessor : DataProcess = new DataProcess(inputDirectories,outputDirectory)

  private val ip: String = "2.2.2.101"
  private val port: Int = 50052


  private def CheckAndParseArgument(args: List[String]): Map[String, List[String]] = {
    assert(args.nonEmpty, s"Too few arguments: ${args.length}")

    val parsedArgs = ParseArgument(Map(), "", args)
    assert(parsedArgs.contains("-O") && parsedArgs("-O").length == 1, s"Invalid arguments: $parsedArgs")

    parsedArgs
  }


  @scala.annotation.tailrec
  private def ParseArgument(map: Map[String, List[String]], previousOption: String, args: List[String]): Map[String, List[String]] = {
    args match {
      case Nil => map

      case arg :: remainder if arg.matches("""\d+\.\d+\.\d+\.\d+:\d+""") =>
        // IP:PORT 형식의 첫 번째 인자를 처리
        val Array(ip, port) = arg.split(":")
        val updatedMap = map + ("masterip" -> List(ip), "masterport" -> List(port))
        ParseArgument(updatedMap, previousOption, remainder)

      case "-I" :: remainder =>
        ParseArgument(map, "-I", remainder)

      case "-O" :: remainder =>
        ParseArgument(map, "-O", remainder)

      case value :: remainder if previousOption.nonEmpty =>
        val updatedMap = map + (previousOption -> (map.getOrElse(previousOption, List()) :+ value))
        ParseArgument(updatedMap, "", remainder)

      case value :: remainder if previousOption.isEmpty =>
        val updatedMap = map + ("UNSPECIFIED" -> (map.getOrElse("UNSPECIFIED", List()) :+ value))
        ParseArgument(updatedMap, "", remainder)
      case _ =>
        println("Unhandled input")
        Map("error" -> List("Invalid argument")) // 기본적으로 error 메시지를 반환
    }
  }

  private val channel: ManagedChannel = {
    ManagedChannelBuilder.forAddress("10.1.25.21" ,50051) // master ip and port
      .usePlaintext()
      .asInstanceOf[ManagedChannelBuilder[_]]
      .build
  }

  logger.info(s"opened worker channel")

  // Get Samples 
  private def sendRegister: Future[Unit] = async {
    val samples: List[Data] = await(this.samples)
    logger.info(s"received sampels")

    val masterChannel: ManagedChannel = channel
    val stub: MasterGrpc.MasterStub = MasterGrpc.stub(masterChannel)
    val request: RegisterRequest = RegisterRequest(ip = NetworkConfig.ip, samples = samples)
    val response: Future[RegisterResponse] = stub.register(request)

    response.onComplete {
      case scala.util.Success(value) =>
        logger.info(s"Register response received: $value")

      case scala.util.Failure(exception) =>
        assert(false, s"Failed to register with Master: ${exception.getMessage}")
    }

    val responseIp = await(response).ip
    assert(responseIp == masterIp, "response IP is not a masterIp")
    logger.info(s"received register response from master $responseIp")
    ()
  }

  private val samples: Future[List[Data]] = dataProcessor.getSamplesFromUnsorted
  sendRegister

  // Shuffling 
  private def prepareRanges(): Future[(SortedMap[String, ByteString], List[String], List[ByteString])] = async {
    val ranges = await(ShuffleStartComplete.future)
    val workerIps = ranges.keys.toList
    val rangeBegins = ranges.values.toList
    (ranges, workerIps, rangeBegins)
  }

  private def prepareData(): Future[(List[BufferedSource], List[Iterator[Data]])] = async {
    val (bufferedSourceList, iteratorList) = await(dataProcessor.getDataFromInputSorted).unzip
    (bufferedSourceList, iteratorList)
  }

  private def prepareGrpcStubs(workerIps: List[String]): (List[ManagedChannel], List[WorkerGrpc.WorkerBlockingStub]) = {

    val workerChannels = workerIps.map { ip =>
      ManagedChannelBuilder.forAddress(ip,50052)
        .usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    }
    
    val workerBlockingStubs = workerChannels.map(WorkerGrpc.blockingStub)
    (workerChannels, workerBlockingStubs)
  }

  private def distributeRecords(
      iteratorList: List[Iterator[Data]],
      rangeBegins: List[ByteString],
      workerBlockingStubs: List[WorkerGrpc.WorkerBlockingStub]
  ): Unit = {
    @tailrec
    def sendShuffleData(records: List[Data]): Unit = {
      if (records.isEmpty) ()
      else {
        val designatedWorker = getDesignatedWorker(records.head, rangeBegins)
        val (dataToSend, remainder) = records.span(record => designatedWorker == getDesignatedWorker(record, rangeBegins))

        val blockingStub = workerBlockingStubs(designatedWorker)
        val request = ShuffleRequest(ip = NetworkConfig.ip, data = dataToSend)
        blockingStub.shuffle(request)

        sendShuffleData(remainder)
      }
    }

    blocking {
      iteratorList.foreach { iter => sendShuffleData(iter.toList) }
    }
  }

  private def getDesignatedWorker(record: Data, rangeBegins: List[ByteString]): Int = {
    val recordKey = record.key
    val blockingStubIdx = rangeBegins.lastIndexWhere(rangeBegin => recordKey >= rangeBegin)
    if (blockingStubIdx == -1) 0 else blockingStubIdx
  }

  private def closeBufferedSources(bufferedSourceList: List[BufferedSource]): Unit = {
    bufferedSourceList.foreach { bufferedSource =>bufferedSource.close()}
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
    val response: Future[ShuffleCompleteResponse] = MasterStub.shuffleComplete(request)

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
    dataProcessor.MergeSortandSave()
  }

  private def sendMergeSortCompleteToMaster: Future[Unit] = async {
    await(MergeSortComplete)
    val masterChannel: ManagedChannel = channel
    val masterStub: MasterGrpc.MasterStub = MasterGrpc.stub(masterChannel)
    val request: MergeSortCompleteRequest = MergeSortCompleteRequest(ip = NetworkConfig.ip)
    val response: Future[MergeSortCompleteResponse] = masterStub.mergeSortComplete(request)

    logger.info(s"Merge Sort complete")
    await(response)
    logger.info(s"Sent Merge Sort Complete Request to master")
    ()
  }

  private val MergeSortStartComplete: Promise[Unit] = Promise()
  private val MergeSortComplete: Future[Unit] = MergeSort
  private val ShuffleAndMergeComplete: Future[Unit] = sendMergeSortCompleteToMaster


  private val server = ServerBuilder.
    forPort(50052)
    .maxInboundMessageSize(4 * DataConfig.writeBlockSize)
    .addService(WorkerGrpc.bindService(new WorkerImpl, ExecutionContext.global))
    .asInstanceOf[ServerBuilder[_]]
    .build
    .start

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



  // Worker 서비스 구현
  private class WorkerImpl extends WorkerGrpc.Worker {
    override def startShuffle(request: ShuffleStartRequest): Future[ShuffleStartResponse] = {
      logger.info(s"Start Shuffle with range: ${request.ranges}")
      ShuffleStartComplete success request.ranges.to(SortedMap)
      Future(ShuffleStartResponse())
    }

    override def shuffle(request: ShuffleRequest): Future[ShuffleResponse] = {
      val records = request.data
      logger.info(s"Received ShuffleRequest")
      dataProcessor saveDistributedData records
      Future(ShuffleResponse())
    }

    override def mergeSortStart(request: MergeSortStartRequest): Future[MergeSortStartResponse] = {
      logger.info(s"Received Merge Sort Start Request")
      MergeSortStartComplete success ()
      Future(MergeSortStartResponse())
    }
  }

}
