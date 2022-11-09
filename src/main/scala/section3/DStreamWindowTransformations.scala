package section3

import common._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Hours
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


object DStreamWindowTransformations {
  /**
    * Boilerplate
    */
  val spark = SparkSession
    .builder()
    .appName("Lesson 3.3 - DStream Window Transformations")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
    * All window durations MUST be a multiple of the Stream Context's batch durations (1 second)
    */
  val ssc = new StreamingContext(sc, Seconds(1))

  private

  def linesStream() = ssc
    .socketTextStream("localhost", 12345)

  /**
    * .window(Seconds(x))
    *
    * - Window duration is x seconds
    * - Each batch reads the last x seconds of data
    * - Window interval is updated with every batch
    * - Window interval must be a multiple of the batch interval
    */
  def linesByWindowStream() =
    linesStream()
      .window(Seconds(10))


  /**
    * .window(Seconds(x), Seconds(y))
    *
    * - Window duration is x seconds
    * - Each batch reads the last x seconds of data
    * - Slide duration is y seconds
    * - y Seconds between batches
    */
  def linesBySlidingWindowStream() =
    linesStream()
      .window(Seconds(10), Seconds(5))


  /**
    * Count the number of elements over a window
    *
    * - Counts the last 60 minutes of data every 30 seconds
    */
  def countLinesByWindowStream() =
    linesStream()
      // .window(Minutes(60), Seconds(30))
      // .count()
      // OR:
      .countByWindow(Minutes(60), Seconds(30))


  /**
    * Count the number of elements over a window
    *
    * - Counts the last 10 seconds of data every 5 seconds
    */
  def sumWindowLinesStream() =
    linesStream()
      .map(_.length)
      .reduce(_ + _)
      .countByWindow(Seconds(10), Seconds(5))
      .reduce(_ + _)

  def sumWindowLinesStreamV2() =
    linesStream()
      .map(_.length)
      .reduceByWindow(
        _ + _,
        Seconds(10),
        Seconds(5)
      )


  /**
    * Tumbling windows
    *
    * - Window duration equals Slide duration
    * - Counts the last 10 seconds of data every 10 seconds
    * - Batch of batches
    */
  def linesByTumblingWindowStream() =
    linesStream()
      .window(Seconds(10), Seconds(10))


  /**
    * .reduceByKeyAndWindow()
    *
    * - Requires a checkpoint directory
    * - (a, b) > a + b: Reduction function: reduces values entering the window
    * - (a, b) => a - b: Inverse function: reduces values leaving the window
    */
  def mostCommonWordsStream() = {
    ssc.checkpoint("checkpoints")

    linesStream()
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b,
        (a, b) => a - b,
        Seconds(60),
        Seconds(30),
      )
  }


  def main(args: Array[String]): Unit = {
    //    val stream = linesByWindowStream()
    //    val stream = linesBySlidingWindowStream()
    //    val stream = countLinesByWindowStream()
    //    val stream = sumWindowLinesStream()
    //    val stream = sumWindowLinesStreamV2()
    //    val stream = linesByTumblingWindowStream()
    val stream = mostCommonWordsStream()

    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
