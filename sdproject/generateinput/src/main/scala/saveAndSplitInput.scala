import java.io.{BufferedReader, BufferedWriter, File, FileReader, FileWriter}
import java.nio.file.{Files, Paths}

object FileChunkSplitter {
  // 분할 기준: 한 청크에 포함할 줄의 수
  private val CHUNK_SIZE = 10000

  def main(args: Array[String]): Unit = {
    // 원본 파일 경로 및 청크 파일을 저장할 디렉토리
    val inputFilePath = "/home/orange/64/input.txt"
    val outputDirPath = "/home/orange/64/input"

    // 청크 파일 저장 디렉토리 생성
    Files.createDirectories(Paths.get(outputDirPath))

    // 파일 분할 수행
    splitFileIntoChunks(inputFilePath, outputDirPath)
  }

  /**
   * 파일을 청크 단위로 분할하는 함수
   *
   * @param inputFilePath 원본 파일 경로
   * @param outputDirPath 청크 파일 저장 디렉토리 경로
   */
  def splitFileIntoChunks(inputFilePath: String, outputDirPath: String): Unit = {
    val reader = new BufferedReader(new FileReader(inputFilePath))
    try {
      var line: String = null
      var chunkLines = Vector.empty[String]
      var chunkIndex = 1

      // 원본 파일을 한 줄씩 읽으며 청크로 분할
      while ({ line = reader.readLine(); line != null }) {
        chunkLines = chunkLines :+ line

        if (chunkLines.size >= CHUNK_SIZE) {
          saveChunkToFile(chunkLines, outputDirPath, chunkIndex)
          chunkLines = Vector.empty[String]
          chunkIndex += 1
        }
      }

      // 남은 줄 처리
      if (chunkLines.nonEmpty) {
        saveChunkToFile(chunkLines, outputDirPath, chunkIndex)
      }

      println(s"파일 분할 완료: $chunkIndex 개의 청크 생성.")
    } catch {
      case ex: Exception => println(s"파일 분할 중 에러 발생: ${ex.getMessage}")
    } finally {
      reader.close()
    }
  }

  /**
   * 청크 데이터를 파일로 저장하는 함수
   *
   * @param lines       청크에 포함된 줄 데이터
   * @param outputDir   청크 파일 저장 디렉토리
   * @param chunkIndex  청크 번호
   */
  private def saveChunkToFile(lines: Vector[String], outputDir: String, chunkIndex: Int): Unit = {
    val chunkFilePath = s"$outputDir/file$chunkIndex.txt"
    val writer = new BufferedWriter(new FileWriter(chunkFilePath))
    try {
      lines.foreach { line =>
        writer.write(line)
        writer.newLine()
      }
      println(s"청크 파일 저장 완료: $chunkFilePath")
    } catch {
      case ex: Exception => println(s"청크 파일 저장 중 에러 발생: ${ex.getMessage}")
    } finally {
      writer.close()
    }
  }
}
