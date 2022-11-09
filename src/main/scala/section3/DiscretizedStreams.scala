package section3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import common.{Stock, buildDataPath}

import java.io.{File, FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

object DiscretizedStreams {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 3.1 - Discretized Streams")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
    * Spark Streaming Context
    *
    * - Entrypoint to the DStream API
    * - Needs a Spark Context and a Duration (batch interval)
    */
  val ssc = new StreamingContext(sc, Seconds(1))

  /**
    * Discretized Streams (DStreams)
    *
    * - Unbound sequence of RDDs
    * - Implement collection operators: map, filter, reduce, etc.
    * - Accessors to ead RDD
    * - Need a Receiver to perform computations
    */

  /**
    * StreamingContext/DStream Workflow
    *
    * 1. Define Input Sources by creating DStreams
    * 2. Define transformations and actions on DStreams
    * 3. Start computations with scc.start();
    * No more computations can be added after this.
    * 4. Await termination or stop the computations
    * We cannot restart the ssc
    */

  def dStreamSocket = {
    /**
      * 1. Create DStreams
      */
    val socketStream: DStream[String] = ssc
      .socketTextStream("localhost", 12345)


    /**
      * 2. Call Transformations and Actions
      *
      * .flatMap() Transformation is lazy
      * .print() Action is eager in the Streaming Context (after ssc.start())
      */
    val wordsStream = socketStream
      .flatMap(_.split(" "))
    wordsStream.print()


    /**
      * 3. Start Computations
      * 4. Await Termination
      *
      * Micro-batches are triggered every second regardless of new data (empty RDDs).
      */
    ssc.start()
    ssc.awaitTermination()
  }

  def addStocksAsync = {
    new Thread(() => {
      val timeoutSeconds = 5
      Thread.sleep(timeoutSeconds * 1000)

      val dirPath = buildDataPath("stocks")
      val dir = new File(dirPath)
      val fileCount = dir.listFiles().length
      val newFile = new File(dirPath + s"/new_stocks_$fileCount.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Apr 1 2002,12.14
          |AAPL,May 1 2002,11.65
          |AAPL,Jun 1 2002,8.86
          |AAPL,Jul 1 2002,7.63
          |AAPL,Aug 1 2002,7.38
          |AAPL,Sep 1 2002,7.25
          |AAPL,Oct 1 2002,8.03
          |AAPL,Nov 1 2002,7.75
        """.stripMargin.trim)

      writer.close()


    }).start()
  }

  def dStreamFromFile = {
    addStocksAsync

    /**
      * 1. Create DStreams
      *
      * .textFileStream() Monitors a directory for new files.
      */
    val stocksPath = buildDataPath("stocks")
    val fileStream: DStream[String] = ssc
      .textFileStream(stocksPath)

    /**
      * 2. Call Transformations
      */
    val dateFormat = new SimpleDateFormat("MMM dd yyyy")

    val stocksStream: DStream[Stock] = fileStream.map { line =>
      val tokens = line.split(",")

      val company = tokens(0)
      val date = new Date(
        dateFormat.parse(tokens(1)).getTime
      )
      val price = tokens(2).toDouble

      Stock(company, date, price)
    }


    /**
      * 2. Call Actions
      */
    stocksStream.print()


    /**
      * 3. Start Computations
      * 4. Await Termination
      *
      * Micro-batches are triggered every second regardless of new data (empty RDDs).
      */
    ssc.start()
    ssc.awaitTermination()
  }

  def dStreamToFile = {
    val socketStream: DStream[String] = ssc
      .socketTextStream("localhost", 12345)

    val wordsStream = socketStream
      .flatMap(_.split(" "))
    wordsStream
      .saveAsTextFiles(buildDataPath("dump/words"))

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {

    //    dStreamSocket
    //    dStreamFromFile
    dStreamToFile
  }
}
