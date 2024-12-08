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
import com.orange.proto.data._
import com.typesafe.scalalogging.Logger
import scala.collection.mutable.ListBuffer
import scala.util.Using

class DataProcess(InputDirectories: List[String], OutputDirectory: String) {
    private val logger: Logger = Logger("DataProcess")

    // 클래스 생성 시 입력 및 출력 디렉토리 정보 로그 출력
    logger.info(s"Data Process initiated")
    logger.info(s"Input Directory passed: $InputDirectories")
    logger.info(s"Output Directory passed: $OutputDirectory")

    // 내부에서 사용하는 임시 디렉토리 정의
    private val SortedInputDirectory: String = s"$OutputDirectory/tmp1"
    private val DistributedDirectory: String = s"$OutputDirectory/tmp2"
    private val directoryHistory: Map[String, AtomicInteger] = Map(
        SortedInputDirectory -> new AtomicInteger(0),
        DistributedDirectory -> new AtomicInteger(0),
        OutputDirectory      -> new AtomicInteger(0)
    )

    // 디렉토리 초기화
    InitializeDirectory(OutputDirectory)
    InitializeDirectory(SortedInputDirectory)
    InitializeDirectory(DistributedDirectory)

    // 입력 디렉토리의 모든 파일 경로 수집
    private val InputPaths: List[String] = InputDirectories.map(getPaths).flatten

    // 입력 데이터를 정렬하는 비동기 작업 정의
    private val SortInputData: Future[List[Unit]] = Future.sequence(InputPaths map (path =>
        Future {
            InternalSortComplete(path, SortedInputDirectory)
        }))

    logger.info(s"Input Directory Initialized: $SortedInputDirectory")
    logger.info(s"Output Directory Initialized: $DistributedDirectory")

    // 디렉토리를 초기화하고 내부 파일 삭제
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

    // 특정 디렉토리 내 파일 경로를 리스트로 반환
    private def getPaths(DirectoryName: String): List[String] = {
        val Directory: File = new File(DirectoryName)
        Option(Directory.listFiles())
          .map(_.toList.map(_.getPath))
          .getOrElse(Nil)
    }

    // 정렬된 입력 파일에서 데이터를 로드하는 함수
    def getDataFromInputSorted: Future[List[(BufferedSource, Iterator[Data])]] = async {
        await(SortInputData)
        val SortedInputFilePaths: List[String] = getPaths(SortedInputDirectory)
        SortedInputFilePaths.map(getData)
    }

    // 정렬되지 않은 입력 데이터에서 샘플을 추출하는 함수
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

    // 셔플된 데이터를 저장하는 함수
    def saveShuffledData(records: Seq[Data]): Unit = {
        saveData(DistributedDirectory, records)
    }

    // 특정 파일에서 데이터를 로드하고 Data 구조체로 변환
    private def getData(fileName: String): (BufferedSource, Iterator[Data]) = {
        val fileContent: BufferedSource = scala.io.Source.fromFile(fileName, "ISO-8859-1")
        val dataIterator: Iterator[Data] = fileContent.grouped(DataConfig.length_data).map(ConvertToData)
        (fileContent, dataIterator)
    }

    // 문자열 데이터를 Data 구조체로 변환
    private def ConvertToData(chr: Seq[Char]): Data = {
        val EntireByte: Array[Byte] = chr.map(_.toByte).toArray
        val key: ByteString = ByteString.copyFrom(EntireByte.take(DataConfig.length_key))
        val value: ByteString = ByteString.copyFrom(EntireByte.drop(DataConfig.length_key))
        Data(key, value)
    }

    // 입력 데이터를 정렬하여 저장
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

    // 데이터를 지정한 디렉토리에 저장
    private def saveData(directory: String, data: Seq[Data]): Unit = {
        val num: Int = directoryHistory(directory).getAndIncrement()
        val file: File = new File(directory + File.separator + f"partition$num%04d")
        val ArrayData: Array[Byte] = data.flatMap(Data => Data.key.toByteArray ++ Data.value.toByteArray).toArray
        Files.write(Paths.get(file.getPath), ArrayData)
    }

    // Merge Sort를 수행하고 결과를 저장
    def MergeSortandSave(): Unit = {
        logger.info(s"Merge Sort Distributed Files")
        val distributedDataDirectory = getPaths(DistributedDirectory)
        val (bufferedSourceList: List[BufferedSource], iteratorList: List[Iterator[Data]]) =
            (distributedDataDirectory.map(getData)).unzip

        logger.info(s"Merge Sort starts")
        val MergeSortResult: Iterator[Data] = MergeSort(iteratorList)
        logger.info(s"Merge Sort completed")

        val SaveMergeSortResult: Iterator[List[Data]] =
            MergeSortResult.grouped(DataConfig.blockConfig).map(_.toList)

        try {
            SaveMergeSortResult.foreach(records => saveData(OutputDirectory, records))
        } finally {
            bufferedSourceList.foreach(_.close())
        }
    }

    // Merge Sort 알고리즘
    private def MergeSort(iteratorList: List[Iterator[Data]]): Iterator[Data] = {
        val (leftIterator, rightIterator) = DividePart(iteratorList, List(), List())

        (leftIterator, rightIterator) match {
            case (Nil, Nil) => Iterator.empty
            case (_, Nil) =>
                if (leftIterator.length != 1) {
                    throw new IllegalStateException("LeftIterator must have exactly one element")
                }
                leftIterator.head
            case (_, _) =>
                val (leftIteratorSorted, rightIteratorSorted) =
                    (MergeSort(leftIterator), MergeSort(rightIterator))
                ConquerPart(leftIteratorSorted, rightIteratorSorted)
        }
    }

    // Iterator 리스트를 분할
    @tailrec
    private def DividePart(iteratorList: List[Iterator[Data]], leftList: List[Iterator[Data]], rightList: List[Iterator[Data]]):
    (List[Iterator[Data]], List[Iterator[Data]]) = {
        iteratorList match {
            case Nil => (leftList, rightList)
            case head :: Nil => (head :: leftList, rightList)
            case first :: second :: remainder => DividePart(remainder, first :: leftList, second :: rightList)
        }
    }

    // 분할된 Iterator를 병합
    private def ConquerPart(leftIteratorSorted: Iterator[Data], rightIteratorSorted: Iterator[Data]): Iterator[Data] = {
        val hasNextLeft = leftIteratorSorted.hasNext
        val leftElement = if (hasNextLeft) Some(leftIteratorSorted.next()) else None

        val hasNextRight = rightIteratorSorted.hasNext
        val rightElement = if (hasNextRight) Some(rightIteratorSorted.next()) else None

        (leftElement, rightElement) match {
            case (None, None) => Iterator.empty
            case (None, Some(iter)) => Iterator.single(iter) ++ rightIteratorSorted
            case (Some(iter), None) => Iterator.single(iter) ++ leftIteratorSorted
            case (Some(leftIter), Some(rightIter)) =>
                if (leftIter.key < rightIter.key)
                    Iterator.single(leftIter) ++ ConquerPart(leftIteratorSorted, Iterator.single(rightIter) ++ rightIteratorSorted)
                else
                    Iterator.single(rightIter) ++ ConquerPart(Iterator.single(leftIter) ++ leftIteratorSorted, rightIteratorSorted)
        }
    }
}
