//worker가 신호 받으면 이 파일의 함수를 호출해서 sorting, shuffle등 진행
package worker
import java.nio.file.{Files.Paths,Path}
import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.annotation.tailrec




class DataProcess(InputDirectories: List[String],OutputDirectory: String){
    private val SortedInputDirectory: String = s"$OutputDirectory/tmp1"
    private val DistributedDirectory: String = s"$OutputDirectory/tmp2"
    private val directoryHistory: Array[(String, AtomicInteger)]= Array(
        SortedInputDirectory -> new AtomicInteger(0),
        DistributedDirectory -> new AtomicInteger(0),
        OutputDirectory      -> new AtomicInteger(0)
    )

    private val InputPaths: List[String] = InputDirectories.map(getPaths).flatten
    private val SortInputData: Future[List[Unit]] = Future.traverse(InputPaths){ path=> 
        Future{x`
            InternalSortComplete(path,SortedInputDirectory)
        }
    }

    //Initialize directory
    InitializeDirectory(OutputDirectory)
    InitializeDirectory(SortedInputDirectory)
    InitializeDirectory(DistributedDirectory)




    private def InitializeDirectory(Directory: String): Unit = {
        val Dir = new File(Directory)
        if (!Dir.exists()) Dir.mkdirs()
        Option(Dir.listFiles()).foreach { files =>
            files.foreach { file =>
            if (!file.isDirectory && !file.delete()) {
                logger.warn(s"Failed to delete file: ${file.getPath}")
            }
            }
        }
    }

    
    private def getPaths(DirectoryName: String): List[String]={
        val Directory: File = new File(DirectoryName)
        Option(Directory.listFiles())
            .map(_.toList.map(_.getPath))
            .getOrElse(Nil)
    }


    def getDataFromInputSorted: Future[List[(BufferedSource,Iterator[Data])]] = async{
        await(SortInputData)
        val SortedInputFilePaths : List[String] = getPaths(SortedInputDirectory)

        val DataBuffer=ListBuffer.empty[(BufferedSource,Iterator[Data])]

        SortedInputFilePaths.foreach{path=>
            DataBuffer += getData(path)
        }

        val result: List[(BufferedSource, Iterator[Data])] = DataBuffer.toList
        result
    }


    //sample들을 반환하는 함수, 원래 교수님이 제시하신 방법은 unsorted된 data에서 sampling
    //우리도 일단 그렇게 구현하는걸 목표로
    //getSamples가 worker machine에서 호출되어 sampling된 data 반환
    /*
    def getSamplesFromUnsorted: Future[List[Data]]=async{
        await(SortInputData)
        if(InputPaths.isEmpty){
            List()
        }
        else{
            val path=InputPaths.head
            val samples: List[Data] = Using(getData(path)._1){fileContent =>
                val dataIterator:Iterator[Data] = getData(path)._2
                dataIterator.take(DataConfig.num_sample).toList
            }.getOrElse(Nil)
            samples
        }
    }
    */

    def getSamplesFromUnsorted: Future[List[Data]] = async {
        await(SortInputData)
        if (InputPaths.isEmpty) {
            List()
        } else {
            val (fileContent, dataIterator) = getData(InputPaths.head)
            val samples = Using(fileContent) { _ =>
            dataIterator.take(DataConfig.num_sample).toList
            }.getOrElse(Nil)
            samples
        }
    }

    //file에서 몇개의 sample들을 추출, 뭔가 file에서 가져오면 string 형태가 될텐데 이걸 Data structure에 맞게 변형하는 함수가 필요할듯
    private def getData(filelName: String): (BufferedSource, Iterator[Data])={
        val fileContent: BufferedSource = scala.io.Source.fromFile(fileName,"ISO-8859-1")
        val dataIterator: Iterator[Data] = fileContent.getLines()
            .flatMap{line => 
                line.grouped(DataConfig.length_data)
                    .map(ConvertToData)
            }
        (fileContent, dataIterator)
    }

    //string을 Data에 맞게 변형
    private def ConvertToData(str: String): Data={
        val EntireByte: Array[Byte] = str.getBytes("ISO-8859-1")
        val key: ByteString = ByteString.copyFrom(EntireByte.take(DataConfig.leng th_key))
        val value: ByteString = ByteString.copyFrom(EntireByte.drop(DataConfig.length_key))
        Data(key,value)
    }

    /*
    private def InternalSortComplete(inputPath: String, saveDirectory: String): Unit = {
        val (fileContent: BufferedSource, dataIterator: Iterator[Data]) = getData(inputPath)
        try{
            for(data <- dataIterator.grouped(DataConfig.blockConfig).map(_.toList)){
                saveData(saveDirectory, Data.sortBy(_.key))
            }
        }finally{
            fileContent.close()
        }
    }
    */

    private def InternalSortComplete(inputPath: String, saveDirectory: String): Unit = {
        val (fileContent, dataIterator) = getData(inputPath)
        try {
            dataIterator.grouped(DataConfig.blockConfig).foreach { group =>
            saveData(saveDirectory, group.sortBy(_.key))
            }
        } finally {
            fileContent.close()
        }
    }


    private def saveData(directory: String, data: Seq[Data]): Unit={
        val directoryEntry = directoryHistory.find(_._1 == directory).getOrElse{
            throw new IllegalArgumentException(s"Directory $directory not found in directories array: ${directories.mkString(", ")}")
        }

        val (path, counter) = directoryEntry
        val num : Int = counter.getAndIncrement()
        val file: File = new File(path + File.seperator + f"partition$num%010d")
        val ArrayData : Array[Byte] = Data.flatMap(Data => Data.key.toByteArray ++ Data.value.toByteArray).toArray

        Files.write(Paths.get(file.getPath), ArrayData)
    }
    def MergeSortandSave(): Unit = {
        logger.info(s"Merge Sort Distributed Files")

        val distributedDataDirectory = getPaths(DistributedDirectory)
        val (bufferedSourceList: List[BufferedSource], IteratorList: List[Iterator[Record]]) =
        (distributedDataDirectory map getData).unzip

        //merge sort 진행 
        logger.info(s"Merge Sort starts")
        val MergeSortResult: Iterator[Record] = MergeSort(IteratorList)
        logger.info(s"Merge Sort completed")

        //merge sort 결과 block 단위로 저장 
        val SaveMergeSortResult: Iterator[List[Record]] =
        MergeSortResult.grouped(RecordConfig.WritableUnit) map (_.toList)

        try {
            SaveMergeSortResult foreach (records => saveData(OutputDirectory, records))
        } finally {
            bufferedSourceList foreach (_.close())
        }
    }

    private def MergeSort (IteratorList: List[Iterator[Record]]): Iterator[Record] = {
        
        val (LeftIterator: List[Iterator[Record]], RightIterator: List[Iterator[Record]]) =
        DividePart(IteratorList, List(), List())

        (LeftIterator, RightIterator) match {
        case (Nil, Nil) => Iterator() //빈 Iterator 반환 

        case (_, Nil) =>
            if (LeftIterator.length != 1) {
                throw new IllegalStateException("LeftIterator must have exactly one element")
            }
            LeftIterator.head


        case (_, _) =>
            val (LeftIteratorSorted: Iterator[Record], RightIteratorSorted: Iterator[Record]) =
            (MergeSort(LeftIterator), MergeSort(RightIterator))
            
            ConquerPart(LeftIteratorSorted, RightIteratorSorted)
        }
    }

    @tailrec
    private DividePart(IteratorList: List[Iterator[Record]], LeftList: List[Iterator[Record]], RightList: List[Iterator[Record]]): 
    (List[Iterator[Record]], List[Iterator[Record]]) = {

        IteratorList match {
            case Nil => (LeftList, RightList)
            case HeadofIterator :: Nil => (HeadofIterator :: LeftList, RightList)
            case FirstIterator :: SecondIterator :: Remainder => DividePart(Remainder, FirstIterator :: LeftList, SecondIterator :: RightList)
        }
    }


    private ConquerPart(LeftIteratorSorted: Iterator[Record], RightIteratorSorted: Iterator[Record]): Iterator[Record] = {

        val hasNextLeft: Boolean = LeftIteratorSorted.hasNext
        val leftElement: Option[Record] = 
            if (hasNextLeft) {
                val element: Record = LeftIteratorSorted.next()
                Some(element)
            } else {
                None
            }

        val hasNextRight: Boolean = RightIteratorSorted.hasNext
        val rightElement: Option[Record] = 
            if (hasNextRight) {
                val element: Record = RightIteratorSorted.next()
                Some(element)
            } else {
                None
            }

        (leftElement, rightElement) match {
            case (None, None) => Iterator()

            case (None, Some(Iter)) => Iterator(Iter) ++ RightIteratorSorted

            case (Some(Iter), None) => Iterator(Iter) ++ LeftIteratorSorted

            case (Some(LeftIter), Some(RightIter)) =>
                if (LeftIter.key < RightIter.key)
                    Iterator(LeftIter) ++ ConquerPart(LeftIteratorSorted, Iterator(RightIter) ++ RightIteratorSorted)
                else
                    Iterator(RightIter) ++ ConquerPart(Iterator(LeftIter) ++ LeftIteratorSorted, RightIteratorSorted)
        }
    }
}
