worker machine에서 수행할 sort를 구현해본다. 

아직 communication 부분이 완벽히 구현되지 않아서 worker-machine에서 청크를 나누어 data를 sort하고 worker machine 내에서 partition을 나누는 과정을 구현해보았다.

후에 communication이 구현되어야 warker-master 통신으로 sampling 과정을 거치고 정확한 partition key를 이용해서 partition 과정을 구현할 수 있을 것이라 생각한다.

```scala
import scala.io.Source
import java.io.{File, PrintWriter}
import scala.concurrent._
import ExecutionContext.Implicits.global
import java.util.concurrent.Executors
import scala.util.Random

object WorkerSortPartition {
  def main(args: Array[String]): Unit = {
    // 입력 파일 경로와 출력 디렉토리 설정
    val inputFilePath = args(0)
    val outputDirPath = args(1)
    val chunkSize = args(2).toInt
    val threadCount = args(3).toInt
    val sampleSize = args(4).toInt // 각 청크에서 샘플링할 데이터 개수

    val outputDir = new File(outputDirPath)
    if (!outputDir.exists()) outputDir.mkdirs()

    // 1. 데이터를 청크 단위로 정렬
    val sortedChunkFiles = parallelSortChunks(inputFilePath, chunkSize, threadCount, outputDir)

    // 2. 샘플링
    val samples = extractSamples(sortedChunkFiles, sampleSize)
    println(s"Extracted samples: ${samples.take(10).mkString(", ")} ...")

    // 3. 파티션 경계 설정 (예: 샘플에서 결정된 파티션 경계, 실제로는 마스터 노드에서 생성)
    val partitions = determinePartitions(samples, 5) // ex) 5개 파티션으로 분할
    println(s"Partitions boundaries: ${partitions.mkString(", ")}")

    // 4. 파티셔닝
    partitionChunks(sortedChunkFiles, partitions, outputDir)
  }

  // 데이터를 멀티스레드로 청크 단위 정렬
  def parallelSortChunks(inputFile: String, chunkSize: Int, threadCount: Int, outputDir: File): Seq[File] = {
    val source = Source.fromFile(inputFile)
    val lines = source.getLines()
    var chunkIndex = 0

    implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threadCount))
    val futures = collection.mutable.Buffer[Future[File]]()

    try {
      while (lines.hasNext) {
        val chunk = lines.take(chunkSize).toList
        val currentIndex = chunkIndex
        chunkIndex += 1

        val future = Future {
          val sortedChunk = chunk.sorted
          val chunkFile = new File(outputDir, s"sorted_chunk_$currentIndex.txt")
          saveToFile(sortedChunk, chunkFile.getPath)
          println(s"Chunk $currentIndex sorted.")
          chunkFile
        }
        futures += future
      }
    } finally {
      source.close()
    }

    Await.result(Future.sequence(futures), duration.Duration.Inf)
  }

  // 각 정렬된 청크에서 샘플링
  def extractSamples(sortedChunkFiles: Seq[File], sampleSize: Int): Seq[String] = {
    sortedChunkFiles.flatMap { file =>
      val lines = Source.fromFile(file).getLines().toList
      val step = math.max(1, lines.size / sampleSize)
      lines.indices.by(step).map(lines)
    }.sorted
  }

  // 샘플 데이터를 기반으로 파티션 경계 결정
  def determinePartitions(samples: Seq[String], partitionCount: Int): Seq[String] = {
    val step = math.max(1, samples.size / partitionCount)
    samples.indices.by(step).map(samples).tail // 첫 파티션 경계 제외
  }

  // 각 청크를 파티션에 따라 나누기
  def partitionChunks(sortedChunkFiles: Seq[File], partitions: Seq[String], outputDir: File): Unit = {
    sortedChunkFiles.foreach { file =>
      val lines = Source.fromFile(file).getLines()
      val partitionWriters = partitions.indices.map { i =>
        new PrintWriter(new File(outputDir, s"partition_${i}_from_${file.getName}"))
      }

      try {
        lines.foreach { line =>
          val partitionIndex = findPartition(line, partitions)
          partitionWriters(partitionIndex).println(line)
        }
      } finally {
        partitionWriters.foreach(_.close())
      }
    }
  }

  // 파티션을 결정하는 함수
  def findPartition(line: String, partitions: Seq[String]): Int = {
    partitions.indexWhere(boundary => line < boundary) match {
      case -1 => partitions.size // 마지막 파티션
      case index => index
    }
  }

  // 데이터를 파일로 저장
  def saveToFile(data: List[String], filePath: String): Unit = {
    val writer = new PrintWriter(new File(filePath))
    try {
      data.foreach(writer.println)
    } finally {
      writer.close()
    }
  }
}
```
