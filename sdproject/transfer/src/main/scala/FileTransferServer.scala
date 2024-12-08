package server

import filetransfer.filetransfer.{Ack, FileChunk, FileTransferServiceGrpc}
import io.grpc.{Server, ServerBuilder}

import java.io.FileOutputStream
import scala.concurrent.{ExecutionContext, Future}

class FileTransferServer extends FileTransferServiceGrpc.FileTransferService {
  override def sendFile(request: FileChunk): Future[Ack] = {
    println(s"Receiving file: ${request.filename}")
    val savePath = s"src/main/scala/server/received_${request.filename.split("[/\\\\]").last}" // 경로 설정
    val output = new FileOutputStream(savePath)
    try {
      output.write(request.data.toByteArray)
      println(s"File ${request.filename} saved successfully.")
    } finally {
      output.close()
    }
    Future.successful(Ack(s"File ${request.filename} received and saved."))
  }
}

object FileTransferServer {
  def main(args: Array[String]): Unit = {
    val server: Server = ServerBuilder
      .forPort(50051)
      .addService(FileTransferServiceGrpc.bindService(new FileTransferServer, ExecutionContext.global))
      .build
      .start
    println("FileTransferServer started, listening on 50051")
    server.awaitTermination()
  }
}
