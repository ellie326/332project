5주차 shuffle 알고리즘 수정 

shuffle algorithm.md 파일에서 초기에 구상한 Sequential 방식(Worker A → Master → Worker B)에서는 데이터의 이동이 Master를 경유하며 병목현상이 발생할 가능성이 클 것 같아 조교님의 피드백에서 제안된 방식처럼 Master는 Pivot 데이터만 전달하고, 실제 데이터의 Shuffle은 Worker들 간의 직접 통신으로 처리하도록 설계를 수정하여 pseudocode 를 작성하였다. 

그 전에 먼저 master 과 worker 의 역할에 대한 정의를 하는 것이 팀원들과 communication 하기 쉬울 것 같아 아래와 같이 정의하였다. 

Master: 
Master는 worker들에게 Pivot 데이터를 계산 및 전달하는 역할만 수행함. 
Pivot 데이터는 Worker 간에 데이터를 어떻게 분배할지를 결정하는 기준임. 

따라서, master 에서 해야하는 계산은 다음과 같다. 
1. 데이터 범위 계산:
   - 전체 데이터를 분석하여 각 Worker로 분배될 데이터의 범위를 결정
   
2. Pivot 전달:
   - 각 Worker에게 Pivot 데이터를 전송
   - Pivot 데이터 = 각 Worker가 처리해야 할 Key 범위


Worker: 
Worker들은 master로부터 받은 pivot 데이터를 기반으로 다른 Worker들과 "직접" 데이터를 교환 -> GRPC 사용

따라서, worker 에서 해야하는 계산은 다음과 같다.

1. 데이터 분할:
   - 각 Worker는 자신의 데이터에서 Pivot 기준으로 데이터를 분할

2. 데이터 전송:
   - Worker들은 다른 Worker로 전송해야 하는 데이터를 parallel 하게 전송
   - gRPC 사용!! 

함수 pseudocode: 

1. shuffle Data

아래에서 설명할 partition data 함수를 통해 master 에서 전달받은 pivot 을 기준으로 데이터를 분할함. (각 worker 내에서 실행) 

그리고, 분할된 데이터를 알맞은 worker 로 데이터 전송. 이때, sendToWorker 함수를 사용하여 비동기적으로 데이터를 전송시킴. 

```scala
def shuffleData(workers: Map[Int, WorkerData], pivots: Array[Int])(implicit ec: ExecutionContext): Unit = {
  workers.par.foreach { case (workerId, workerData) =>
    // 1. 데이터를 분할
    val partitionedData = workerData.partitionData(pivots)

    // 2. 다른 Worker로 데이터 전송
    partitionedData.zipWithIndex.foreach { case (dataChunk, index) =>
      val targetWorkerId = index + 1
      if (targetWorkerId != workerId) {
        // 3. 비동기 데이터 전송
        sendToWorker(targetWorkerId, dataChunk)
      }
    }
  }
}
```

위에서 언급한 sendToWorker 는 전송하려는 worker 의 ID 를 기반으로 데이터를 key-value 형태로 gRPC 메시지로 전송. 이때, 전송 받아야하는 worker 에 데이터를 저장할 수 없다면 일정 시간 이후 재시도를 하는 로직을 추가하였다. 

```scala
def sendToWorker(
  workerId: Int,
  data: Map[String, Int],
  maxRetries: Int = 3, // 최대 재시도 횟수
  retryDelay: Int = 1000 // 재시도 간격 (밀리초 단위)
)(implicit ec: ExecutionContext, workers: Map[Int, String]): Future[Boolean] = {
  val workerAddress = workers.getOrElse(workerId, throw new IllegalArgumentException(s"Worker $workerId not found"))
  val channel = ManagedChannelBuilder.forTarget(workerAddress).usePlaintext().build()
  val stub = ShuffleServiceGrpc.stub(channel)

  val request = TransferRequest(
    sender_id = -1,
    receiver_id = workerId,
    node_type = "data",
    key_value_pairs = data.toSeq.map { case (k, v) => shuffle.KeyValue(k, v) }
  )

  def attemptTransfer(retryCount: Int): Future[Boolean] = {
    stub.transferData(request).map(_.success).recoverWith { case ex =>
      if (retryCount < maxRetries) {
        println(s"Retrying to send data to Worker $workerId (Retry: ${retryCount + 1})...")
        Thread.sleep(retryDelay) // 재시도 간격
        attemptTransfer(retryCount + 1) // 재귀적으로 재시도
      } else {
        println(s"Failed to send data to Worker $workerId after $maxRetries retries.")
        Future.successful(false) // 모든 재시도 실패
      }
    }
  }

  attemptTransfer(0).andThen { case _ =>
    channel.shutdown() // 작업 완료 후 채널 닫기
  }
}

```

마지막으로 partitionData 함수는 worker 내에서 pivot 을 기준으로 data 들을 분할하는 함수이다. 

```scala
class WorkerData(data: Map[String, Int]) {
  def partitionData(pivots: Array[Int]): Array[Map[String, Int]] = {
    val partitions = Array.fill(pivots.length + 1)(mutable.Map[String, Int]())
    data.foreach { case (key, value) =>
      val index = pivots.indexWhere(pivot => key.toInt < pivot)
      partitions(if (index == -1) pivots.length else index) += (key -> value)
    }
    partitions.map(_.toMap)
  }
}
```


