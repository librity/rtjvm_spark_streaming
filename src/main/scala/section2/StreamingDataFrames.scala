package section2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.{buildDataPath, buildJsonPath, inspect, readJson, defaultSocketConfig, stocksSchema}
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

object StreamingDataFrames {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 2.1 - Streaming Data Frames")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def readFromSocket = {
    /**
      * Read Stream as Data Frame
      */
    val lines = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()

    println(s"Is streaming? ${lines.isStreaming}")

    /**
      * Filter
      *
      * For the most part, we can manipulate Stream Data Frames
      * as we would Static ones.
      */
    val shortLines = lines
      .where(length($"value") <= 5)


    /**
      * Printer
      */
    val query = shortLines
      .writeStream
      .format("console")
      .outputMode("append")
      .start()


    /**
      * Listen for input
      */
    query.awaitTermination()
  }

  def readFromFile = {
    /**
      * MUST specify a schema for Streaming Data Frames
      */
    val stocks = spark
      .readStream
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .schema(stocksSchema)
      .csv(buildDataPath("stocks"))

    stocks
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers(myTrigger: Trigger) = {
    val lines = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()

    lines
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(myTrigger)
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /**
      * Adding a new file to stocks folder while listening will trigger another batch!
      */
    //    readFromFile

    /**
      * Run "nc -lk 12345" and type stuff before running.
      */
    //    readFromSocket

    /**
      * Trigger.ProcessingTime()
      *
      * Run the query every two seconds if the source has new data (batch every two seconds).
      */
    //    demoTriggers(Trigger.ProcessingTime(2.seconds))

    /**
      * Trigger.Once
      *
      * Single batch, then terminate.
      */
    //    demoTriggers(Trigger.Once)

    /**
      * Trigger.Continuous() (EXPERIMENTAL)
      *
      * Run the query every two seconds regardless of whether the source has new data.
      */
    //    demoTriggers(Trigger.Continuous(2.seconds))
  }
}
