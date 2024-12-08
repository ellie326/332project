package client

import filetransfer.filetransfer.{FileChunk, FileTransferServiceGrpc}
import io.grpc.ManagedChannelBuilder

import java.nio.file.{Files, Paths}

object FileTransferClient {
  def main(args: Array[String]): Unit = {
    val channel = ManagedChannelBuilder.forAddress("10.1.25.21", 50051).usePlaintext().build
    val blockingStub = FileTransferServiceGrpc.blockingStub(channel)

    val filename = "/home/orange/test1/output_dir/partition0000000000.txt"
    val filePath = Paths.get(filename)
    if (Files.exists(filePath)) {
      val fileBytes = Files.readAllBytes(filePath)
      val request = FileChunk(filename = filename, data = com.google.protobuf.ByteString.copyFrom(fileBytes))

      val response = blockingStub.sendFile(request)
      println(response.message)
    } else {
      println(s"File $filename does not exist.")
    }

    channel.shutdown()
  }
}
