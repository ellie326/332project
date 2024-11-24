import io.grpc.{Server, ServerBuilder}
import scala.concurrent.{ExecutionContext, Future}
import master.MasterGrpc
import master.{RegisterRequest, RegisterResponse}

object Master extends App {
  private val ip: String = "141.223.16.227"
  private val port: Int = 7777
  private val workerNum: Int = 3  // 예시로 3명의 워커가 등록되면 시작

  // 워커 등록을 위한 리스트
  private var workerIps: List[String] = List()

  // gRPC 서버 설정 및 시작
  private val server = ServerBuilder
    .forPort(port)
    .addService(MasterGrpc.bindService(new MasterImpl, ExecutionContext.global))  // 생성된 서비스 추가
    .build
    .start

  println(s"$ip:$port")

  // 서버 종료 시 클린업
  sys.addShutdownHook {
    println("Shutting down server...")
    server.shutdown()
  }

  // 서버 계속 실행
  server.awaitTermination()

  // gRPC 서비스 구현
  private class MasterImpl extends MasterGrpc.Master {
    override def register(request: RegisterRequest): Future[RegisterResponse] = {
      println(s"Register request from ${request.ip}")
      workerIps = (workerIps :+ request.ip).distinct.sorted  // 워커 IP 등록

      // 모든 워커가 등록되면 워커 IP 리스트 출력
      if (workerIps.length == workerNum) {
        println(s"Sorted Worker IPs: ${workerIps.mkString(", ")}")
      }
      Future.successful(RegisterResponse(ip = ip))  // 응답 반환
    }
  }
}