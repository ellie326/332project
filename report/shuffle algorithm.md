This doc is about shuffle in ppt 12 page. 

![image](https://github.com/user-attachments/assets/0bbf90d1-acda-4b6e-8478-b05724a11f59)

위의 사진에서 보이다시피 각 worker 에서 sort 된 데이터들을 각 구간에 알맞는 worker 로 데이터를 이동하는 과정의 pseudocode 에 대해서 살펴볼 것이다. 

1. gRPC 서버 인터페이스

```
syntax = "proto3";

service ShuffleService {
    rpc TransferData (TransferRequest) returns (TransferResponse);
}

message TransferRequest {
    int32 sender_id = 1;
    int32 receiver_id = 2;
    string node_type = 3;
    map<string, string> key_value_pairs = 4; // key-value 데이터를 위한 맵
}

message TransferResponse {
    bool success = 1;
    string message = 2;
}
```
worker 와 master 간의 소통을 위해 gRPC 사용. 
serder_id = 데이터를 보내는 워커의 ID 
receiver_id = 데이터를 받는 워커의 ID 
node_type = 전송하려는 node 유형 (예를 들어 node A, B, C 가 각각 전체 데이터의 33%를 나타낸다고 했을 때, 이 node 의 id 를 말함) 
amount = 전송하려는 데이터의 양 


2. shuffle
```
import io.grpc.{Server, ServerBuilder}
import shuffle.{ShuffleServiceGrpc, TransferRequest, TransferResponse} 
import scala.concurrent.Future
import scala.collection.mutable

// ShuffleService 서버 구현
class ShuffleServiceImpl(workers: mutable.Map[Int, WorkerData], targetNode: Map[Int, String])
  extends ShuffleServiceGrpc.ShuffleService {

  override def transferData(request: TransferRequest): Future[TransferResponse] = {
    val sender = request.sender_id
    val receiver = request.receiver_id
    val nodeType = request.node_type
    val keyValuePairs = request.key_value_pairs

    // 교환 처리
    if (workers.contains(sender) && workers.contains(receiver)) {
      workers(sender).decrease(nodeType, keyValuePairs.toMap)
      workers(receiver).increase(nodeType, keyValuePairs.toMap)
      Future.successful(TransferResponse(success = true, s"Transferred ${keyValuePairs.size} items of $nodeType from Worker $sender to Worker $receiver"))
    } else {
      Future.successful(TransferResponse(success = false, "Invalid sender or receiver"))
    }
  }
}
```

transferData 함수에서는 sender 와 receiver 가 유효한 워커인지 확인한 후, sender 에서 data 를 shuffle 시키는 만큼 delete 하고, receiver 에서는 그만큼 insert 시켜주어야한다. 이를 구현하기 위한 decrease 와 increase 함수는 아래의 workerData class 에서 설명할 예정이다. 이후, 성공 여부를 보여주는 형식으로 pseudocode 를 작성해보았다.  

** 여기서 아직 확실하지 않는 부분이 만약 worker A 에서 worker B 로 데이터를 옮긴다고 가정했을 때, 
worker A-> master && worker B -> master
then, master -> workerA, workerB 
이렇게 두 단계로 진행될텐데 master 를 총 worker 의 갯수로 나눈다음 만약 master 가 100GB 고, worker 가 10개라면 10GB 에 해당하는 데이터의 양만큼을 반복적으로 바꿔야할지?

아니면 worker A -> master 
then worker B -> worker A 
finally, master -> worker B  
이렇게 세 단계로 진행해야하는지? 

이렇게 했을 때, sequentially 진행하게 될텐데 (write 을 동시에 할 수 없으니깐) parallel 하게 진행한다면 어떻게 진행해야하는지? 

worker 갯수를 알 수 있는지?? 

-> 다음 팀별 미팅때 같이 얘기해본 뒤 해당하는 코드 작성 시작하면 될 것 같다. 

```
if (workers.contains(sender) && workers.contains(receiver)) {
  // 교환할 최대 `key-value` 쌍 수를 정합니다. 예를 들어, 10개로 제한.
  val maxExchangeCount = 10
  
  // 실제 전송할 `key-value` 쌍을 `keyValuePairs`에서 선택합니다.
  val selectedPairs = keyValuePairs.toMap.take(maxExchangeCount)
  
  // 송신자에서 `selectedPairs`의 키-값 쌍을 제거하고, 수신자에 추가합니다.
  workers(sender).decrease(nodeType, selectedPairs)
  workers(receiver).increase(nodeType, selectedPairs)

  // 전송한 항목 수와 정보를 반환합니다.
  Future.successful(TransferResponse(success = true, s"Transferred ${selectedPairs.size} items of $nodeType from Worker $sender to Worker $receiver"))
} else {
  Future.successful(TransferResponse(success = false, "Invalid sender or receiver"))
}
```

갯수만큼만 수정한다고 가정했을때 위와 같이 수정하면 될 것이다.

```
// 워커 데이터 모델
case class WorkerData(data: mutable.Map[String, mutable.Map[String, String]]) {
  def decrease(nodeType: String, keyValuePairs: Map[String, String]): Unit = {
    keyValuePairs.foreach { case (key, _) =>
      data(nodeType).remove(key)
    }
  }

  def increase(nodeType: String, keyValuePairs: Map[String, String]): Unit = {
    keyValuePairs.foreach { case (key, value) =>
      data(nodeType).put(key, value)
    }
  }
}
```

decrease 와 increase 함수에서는 해당하는 key-value 쌍의 데이터를 제거 및 추가할 수 있도록 하였다. 

```
object ShuffleServiceServer {
  def start(workers: mutable.Map[Int, WorkerData], targetNode: Map[Int, String], port: Int): Unit = {
    val server = ServerBuilder
      .forPort(port)
      .addService(ShuffleServiceGrpc.bindService(new ShuffleServiceImpl(workers, targetNode), scala.concurrent.ExecutionContext.global))
      .build()
      .start()

    println(s"Server started, listening on $port")
    server.awaitTermination()
  }
}
```

위의 객체는 server 를 build 하고 시작하는 과정을 나타낸다. 

3. worker client
```
import io.grpc.ManagedChannelBuilder
import shuffle.{ShuffleServiceGrpc, TransferRequest}

class Worker(workerId: Int, initialData: Map[String, Map[String, String]], targetNode: String, serverAddresses: Map[Int, String]) {
  private val data = mutable.Map(initialData.mapValues(mutable.Map(_: _*)))

  // 필요한 데이터를 다른 워커에게 요청
  def transferData(receiverId: Int, nodeType: String, keyValuePairs: Map[String, String]): Unit = {
    val channel = ManagedChannelBuilder.forAddress(serverAddresses(receiverId), 50051).usePlaintext().build()
    val stub = ShuffleServiceGrpc.blockingStub(channel)

    val request = TransferRequest(sender_id = workerId, receiver_id = receiverId, node_type = nodeType, key_value_pairs = keyValuePairs)
    val response = stub.transferData(request)
    println(response.message)

    channel.shutdown()
  }

  // 현재 상태 출력
  def printState(): Unit = {
    println(s"Worker $workerId 상태: $data")
  }
}
```

각 worker 가 본인이 갖고 있는 data 들을 관리하고 gRPC 를 통해서 다른 worker 에게 shuffle 을 요청하는 과정을 갖고 있음. 


4. shuffle manager 
```
import scala.collection.mutable

object ShuffleManager {
  def balanceData(workers: Map[Int, Worker], targetNodePerWorker: Map[Int, String]): Unit = {
    var exchanges = true

    while (exchanges) {
      exchanges = false

      // 각 워커에 대해 필요한 데이터가 있는지 확인하고 교환
      for ((workerId, worker) <- workers) {
        val targetNode = targetNodePerWorker(workerId)

        worker.printState()

        // 워커가 현재 필요한 노드가 아닌 데이터의 키-값 쌍을 가져옵니다.
        for ((nodeType, keyValuePairs) <- worker.data if nodeType != targetNode && keyValuePairs.nonEmpty) {
          // 해당 노드를 필요로 하는 다른 워커를 찾고, 일부 데이터를 교환
          for ((otherWorkerId, otherWorker) <- workers if otherWorkerId != workerId) {
            if (targetNodePerWorker(otherWorkerId) == nodeType) {
              // 교환할 데이터 준비: 다른 워커가 필요한 key-value 쌍
              val transferPairs = keyValuePairs.take(10) // 한 번에 최대 10개의 key-value 쌍을 전송 (임의 설정)

              if (transferPairs.nonEmpty) {
                // 데이터를 서로 전송하여 교환
                worker.transferData(otherWorkerId, nodeType, transferPairs)
                otherWorker.transferData(workerId, targetNode, transferPairs)

                // `exchanges`를 true로 설정하여 교환이 발생했음을 표시
                exchanges = true
              }
            }
          }
        }
      }
    }

    // 최종 상태 출력
    workers.values.foreach(_.printState())
  }
}
```

```
  +) 추가 : 
  def main(args: Array[String]): Unit = {
    // 각 워커의 초기 데이터 설정 (key-value 형태로 저장)
    val initialWorkers = Map(
      0 -> new Worker(0, Map("nodeA" -> Map("key1" -> "value1"), "nodeB" -> Map("key2" -> "value2", "key3" -> "value3"), "nodeC" -> Map("key4" -> "value4")), "nodeA", serverAddresses),
      1 -> new Worker(1, Map("nodeA" -> Map("key5" -> "value5"), "nodeB" -> Map("key6" -> "value6", "key7" -> "value7"), "nodeC" -> Map("key8" -> "value8")), "nodeB", serverAddresses),
      2 -> new Worker(2, Map("nodeA" -> Map("key9" -> "value9"), "nodeB" -> Map("key10" -> "value10"), "nodeC" -> Map("key11" -> "value11")), "nodeC", serverAddresses)
    )

    val targetNodes = Map(0 -> "nodeA", 1 -> "nodeB", 2 -> "nodeC")
    balanceData(initialWorkers, targetNodes)
  }
}

```
