ppt 기준 이번 프로젝트에서는 크게 네가지 단계가 있는 것으로 보인다. 
각 단계별로 gpt 를 사용해서 pseudocode 를 작성해보았다. 

### 1. 데이터 생성 (gensort, ppt 3페이지 참고) 
먼저, 올바른 데이터 포맷으로 사용되는지를 확인해보아야한다. 
각 데이터는 길이가 100바이트이고, 10 byte 는 key 로 사용, 90 byte는 value 로 사용한다) 

```
import java.io._

def generateData(filePath: String, numRecords: Int): Unit = {
  val file = new File(filePath)
  val writer = new BufferedWriter(new FileWriter(file))
  for (_ <- 1 to numRecords) {
    val key = (1 to 10).map(_ => ('A' + scala.util.Random.nextInt(26)).toChar).mkString
    val value = (1 to 90).map(_ => ('A' + scala.util.Random.nextInt(26)).toChar).mkString
    writer.write(key + value + "\n")
  }
  writer.close()
}
```

### 2. 로컬에서 데이터 정렬 (ppt 9페이지 참고) 
그 다음 첫번째 challenge 가 주어진다. 로컬 메모리가 부족하면 데이터를 chunck 로 나눠서 각 chuck 별로 정렬 한 다음 합쳐주는 과정이 필요하다 
아래의 코드에서 각 chunck 를 정렬한 다음 file 에 저장하는 과정에 대한 디자인을 작성하였다. 
```
import scala.io.Source
import java.io._

def sortChunk(inputFilePath: String, outputFilePath: String): Unit = {
  val lines = Source.fromFile(inputFilePath).getLines().toSeq
  val sortedLines = lines.sortBy(_.substring(0, 10)) 
  val writer = new BufferedWriter(new FileWriter(outputFilePath))
  sortedLines.foreach(line => writer.write(line + "\n"))
  writer.close()
}
```

### 3. Distributed sorting 
위에서 정렬된 파일들을 합해서 저장해주어야하는데 각 파일별로 첫 data 를 확인하여 가장 작은 key 를 가진 data 를 result file 에 작성하는 방식으로 구현해보고자 한다. 
이미 각 파일들은 정렬된 상태일 것이기 때문에 각 파일의 첫 data 만 확인해도 된다.
```
import scala.collection.mutable
import java.io._

case class Record(line: String, fileIndex: Int)

def mergeSortedFiles(sortedFiles: List[String], outputFilePath: String): Unit = {
  val readers = sortedFiles.map(new BufferedReader(new FileReader(_)))
  val pq = mutable.PriorityQueue[Record]()(Ordering.by(_.line.substring(0, 10)).reverse) // 키 기준으로 우선순위 큐
  readers.zipWithIndex.foreach { case (reader, index) =>
    val line = reader.readLine()
    if (line != null) pq.enqueue(Record(line, index))
  }

  val writer = new BufferedWriter(new FileWriter(outputFilePath))
  while (pq.nonEmpty) {
    val minRecord = pq.dequeue()
    writer.write(minRecord.line + "\n")
    val nextLine = readers(minRecord.fileIndex).readLine()
    if (nextLine != null) pq.enqueue(Record(nextLine, minRecord.fileIndex))
  }
  readers.foreach(_.close())
  writer.close()
}
```

### 4. master-worker 구현 (ppt 16페이지 참고) 
마지막으로 java.net 을 사용해 마스터가 각 worker 에게 작업을 분배하는 과정이 필요하다. 
master는 각 worker 에게 특정 data 를 정렬하도록 할당할 것이고 worker 는 정렬 후 master 가 지정한 파일에 결과를 저장할 것이다. 
master:  
```
import java.net.ServerSocket
import java.io._

class Master(port: Int) {
  val server = new ServerSocket(port)
  def start(): Unit = {
    println(s"Master listening on port $port")
    while (true) {
      val socket = server.accept()
      new Thread(new WorkerHandler(socket)).start()
    }
  }
}

class WorkerHandler(socket: Socket) extends Runnable {
  def run(): Unit = {
    val in = new BufferedReader(new InputStreamReader(socket.getInputStream))
    val out = new PrintWriter(socket.getOutputStream, true)
    val task = in.readLine()
    // 작업 처리 
    out.println("Task completed")
    socket.close()
  }
}
```

worker: 
```
import java.net.Socket
import java.io._

class Worker(masterIp: String, masterPort: Int, inputDir: String, outputDir: String) {
  def start(): Unit = {
    val socket = new Socket(masterIp, masterPort)
    val out = new PrintWriter(socket.getOutputStream, true)
    out.println("Requesting task")
    // 작업 수신 및 처리
    socket.close()
  }
}
```

위의 코드들은 임시로 pseudocode 를 ai 툴을 이용하여 작성한 것이기 때문에 디테일한 부분은 수정이 필요하겠지만 전체적인 흐름은 비슷 할 것으로 예상된다. 
