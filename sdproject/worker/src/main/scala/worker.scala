import io.grpc.{ManagedChannelBuilder, Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import worker.WorkerGrpc
import worker.{DistributeStartRequest, DistributeStartResponse, SortStartRequest, SortStartResponse}

object Worker extends App {
  private val ip: String = "127.0.0.1"
  private val port: Int = 60051
  private val masterIp: String = "127.0.0.1"
  private val masterPort: Int = 50051

  // Master 서버에 연결
  private val channel = ManagedChannelBuilder
    .forAddress(masterIp, masterPort)
    .usePlaintext()
    .build()

  private val masterStub = MasterGrpc.blockingStub(channel)

  // Master에 등록
  val registerRequest = RegisterRequest(ip = ip)
  val registerResponse = masterStub.register(registerRequest)
  println(s"Registered with Master: ${registerResponse.ip}")

  // Worker 서버 시작
  private val server = ServerBuilder
    .forPort(port)
    .addService(WorkerGrpc.bindService(new WorkerImpl, ExecutionContext.global))  // 생성된 서비스 추가
    .build
    .start()

  println(s"Worker started at $ip:$port")

  // 종료 시 서버 클린업
  sys.addShutdownHook {
    println("Shutting down worker...")
    server.shutdown()
  }

  server.awaitTermination()

  // Worker 서비스 구현
  private class WorkerImpl extends WorkerGrpc.Worker {
    override def distributeStart(request: DistributeStartRequest): Future[DistributeStartResponse] = {
      println(s"Received distribute start request with ranges: ${request.ranges}")
      Future.successful(DistributeStartResponse())
    }

    override def sortStart(request: SortStartRequest): Future[SortStartResponse] = {
      println("Received sort start request")
      // 정렬 작업 시뮬레이션
      Thread.sleep(1000)
      println("Sorting complete")
      Future.successful(SortStartResponse())
    }
  }
}