package com.orange.worker

import com.orange.bytestring_ordering.ByteStringOrdering._

import java.util.concurrent.atomic.AtomicInteger
import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.annotation.tailrec
import scala.io._
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.async.Async.{async, await}
import com.google.protobuf.ByteString
import com.orange.bytestring_ordering.ByteStringOrdering._
import scala.math.Ordered.orderingToOrdered

import com.orange.proto.data._
import com.typesafe.scalalogging.Logger


import scala.collection.mutable.ListBuffer
import scala.util.Using




class DataProcess(InputDirectories: List[String],OutputDirectory: String){
    private val logger: Logger = Logger("DataProcess")

    logger.info(s"Data Process initiated")
    logger.info(s"Input Directory passed: $InputDirectories")
    logger.info(s"Output Directory passed: $OutputDirectory")

    private val SortedInputDirectory: String = s"$OutputDirectory/tmp1"
    private val DistributedDirectory: String = s"$OutputDirectory/tmp2"
    private val directoryHistory: Map[String, AtomicInteger]= Map(
        SortedInputDirectory -> new AtomicInteger(0),
        DistributedDirectory -> new AtomicInteger(0),
        OutputDirectory      -> new AtomicInteger(0)
    )

    //Initialize directory
    InitializeDirectory(OutputDirectory)
    InitializeDirectory(SortedInputDirectory)
    InitializeDirectory(DistributedDirectory)

    private val InputPaths: List[String] = InputDirectories.map(getPaths).flatten
    private val SortInputData: Future[List[Unit]] = Future.sequence(InputPaths map( path=>
        Future {
            InternalSortComplete(path, SortedInputDirectory)
        }))


    logger.info(s"Input Directory Initialized: $SortedInputDirectory")
    logger.info(s"Output Directory Initialized: $DistributedDirectory")


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

        SortedInputFilePaths map getData
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
        logger.info(s"Get samples from unsorted")
        await(SortInputData)
        if (InputPaths.isEmpty) {
            logger.info(s"Input path is empty")
            List()
        } else {
            logger.info(s"Sampling Starts from ${InputPaths.head}")
            val (fileContent, dataIterator) = getData(InputPaths.head)
            val samples = Using(fileContent) { _ =>
            dataIterator.take(DataConfig.num_sample).toList
            }.getOrElse(Nil)
            logger.info(s"Sampling Completed. ${samples.length} samples had been considered")
            samples
        }
    }

    def saveDistributedData(records: Seq[Data]): Unit = {
        saveData(DistributedDirectory, records)
    }

    //file에서 몇개의 sample들을 추출, 뭔가 file에서 가져오면 string 형태가 될텐데 이걸 Data structure에 맞게 변형하는 함수가 필요할듯
    private def getData(fileName: String): (BufferedSource, Iterator[Data])={
        val fileContent: BufferedSource = scala.io.Source.fromFile(fileName,"ISO-8859-1")
        val dataIterator: Iterator[Data] = fileContent.grouped(DataConfig.length_data) map ConvertToData

        (fileContent, dataIterator)
    }

    //string을 Data에 맞게 변형
    private def ConvertToData(chr: Seq[Char]): Data={
        val EntireByte: Array[Byte] = chr.map(_.toByte).toArray
        val key: ByteString = ByteString.copyFrom(EntireByte.take(DataConfig.length_key))
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

        val num : Int = directoryHistory(directory).getAndIncrement()
        val file: File = new File(directory + File.separator + f"partition$num%010d")
        val ArrayData : Array[Byte] = data.flatMap(Data => Data.key.toByteArray ++ Data.value.toByteArray).toArray

        Files.write(Paths.get(file.getPath), ArrayData)
    }

    def MergeSortandSave(): Unit = {
        logger.info(s"Merge Sort Distributed Files")


        val distributedDataDirectory = getPaths(DistributedDirectory)
        val (bufferedSourceList: List[BufferedSource], iteratorList: List[Iterator[Data]]) =
        (distributedDataDirectory map getData).unzip

        //merge sort 진행 
        logger.info(s"Merge Sort starts")
        val MergeSortResult: Iterator[Data] = MergeSort(iteratorList)
        logger.info(s"Merge Sort completed")

        //merge sort 결과 block 단위로 저장 
        val SaveMergeSortResult: Iterator[List[Data]] =
        MergeSortResult.grouped(DataConfig.blockConfig) map (_.toList)

        try {
            SaveMergeSortResult foreach (records => saveData(OutputDirectory, records))
        } finally {
            bufferedSourceList foreach (_.close())
        }
    }

    private def MergeSort (iteratorList: List[Iterator[Data]]): Iterator[Data] = {
        
        val (leftIterator: List[Iterator[Data]], rightIterator: List[Iterator[Data]]) =
        DividePart(iteratorList, List(), List())

        (leftIterator, rightIterator) match {
        case (Nil, Nil) => Iterator() //빈 Iterator 반환 

        case (_, Nil) =>
            if (leftIterator.length != 1) {
                throw new IllegalStateException("LeftIterator must have exactly one element")
            }
            leftIterator.head


        case (_, _) =>
            val (leftIteratorSorted: Iterator[Data], rightIteratorSorted: Iterator[Data]) =
            (MergeSort(leftIterator), MergeSort(rightIterator))
            
            ConquerPart(leftIteratorSorted, rightIteratorSorted)
        }
    }

    @tailrec
    private def DividePart(iteratorList: List[Iterator[Data]], leftList: List[Iterator[Data]], rightList: List[Iterator[Data]]): 
    (List[Iterator[Data]], List[Iterator[Data]]) = {

        iteratorList match {
            case Nil => (leftList, rightList)
            case headofIterator :: Nil => (headofIterator :: leftList, rightList)
            case firstIterator :: secondIterator :: remainder => DividePart(remainder, firstIterator :: leftList, secondIterator :: rightList)
        }
    }


    private def ConquerPart(leftIteratorSorted: Iterator[Data], rightIteratorSorted: Iterator[Data]): Iterator[Data] = {

        val hasNextLeft: Boolean = leftIteratorSorted.hasNext
        val leftElement: Option[Data] = 
            if (hasNextLeft) {
                val element: Data = leftIteratorSorted.next()
                Some(element)
            } else {
                None
            }

        val hasNextRight: Boolean = rightIteratorSorted.hasNext
        val rightElement: Option[Data] = 
            if (hasNextRight) {
                val element: Data = rightIteratorSorted.next()
                Some(element)
            } else {
                None
            }

        (leftElement, rightElement) match {
            case (None, None) => Iterator()

            case (None, Some(iter)) => Iterator(iter) ++ rightIteratorSorted

            case (Some(iter), None) => Iterator(iter) ++ leftIteratorSorted

            case (Some(leftIter), Some(rightIter)) =>
                if (leftIter.key < rightIter.key)
                    Iterator(leftIter) ++ ConquerPart(leftIteratorSorted, Iterator(rightIter) ++ rightIteratorSorted)
                else
                    Iterator(rightIter) ++ ConquerPart(Iterator(leftIter) ++ leftIteratorSorted, rightIteratorSorted)
        }
    }
}
