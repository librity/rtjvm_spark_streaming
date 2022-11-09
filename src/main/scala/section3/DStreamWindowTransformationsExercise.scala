package section3

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


object DStreamWindowTransformationsExercise {
  /**
    * Boilerplate
    */
  val spark = SparkSession
    .builder()
    .appName("Lesson 3.3 - DStream Window Transformations Exercise")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Exercise
    *
    * - Each long word (>= 10) is worth 2 dollars
    * - Each short word is worth 0 dollars
    * - Read a text stream from localhost as words
    * - Aggregate the total $ value of last 30 seEconds every 10 seconds
    * - Do one version with each Window function:
    */
  val ssc = new StreamingContext(sc, Seconds(10))
  ssc.checkpoint("checkpoints")

  val longWordValue = 2

  private

  def socketStream = ssc
    .socketTextStream("localhost", 12345)

  /**
    * 1) .window()
    */
  def windowValueStream =
    socketStream
      .window(Seconds(30))
      .flatMap(_.split(" "))
      .map { word => if (word.length >= 10) 2 else 0 }
      .reduce(_ + _)


  /**
    * Daniel's Solution
    *
    * - Reduce before and after the window for better performance.
    */
  def windowValueStreamV2 =
    socketStream
      .flatMap(_.split(" "))
      .filter(_.length >= 10)
      .map(_ => longWordValue)
      .reduce(_ + _)
      .window(Seconds(30), Seconds(10))
      .reduce(_ + _)


  /**
    * 2) .countByWindow()
    */
  def countByWindowValueStream =
    socketStream
      .flatMap(_.split(" "))
      .flatMap { word => if (word.length >= 10) Seq(1, 1) else Seq() }
      .countByWindow(Seconds(30), Seconds(10))


  /**
    * Daniel's Solution
    */
  def countByWindowValueStreamV2 =
    socketStream
      .flatMap(_.split(" "))
      .filter(_.length >= 10)
      .countByWindow(Seconds(30), Seconds(10))
      .map(_ * longWordValue)

  /**
    * 3) .reduceByWindow()
    */
  def reduceByWindowValueStream =
    socketStream
      .flatMap(_.split(" "))
      .map { word => if (word.length >= 10) 2 else 0 }
      .reduceByWindow(
        _ + _,
        Seconds(30),
        Seconds(10),
      )


  /**
    * Daniel's Solution
    */
  def reduceByWindowValueStreamV2 =
    socketStream
      .flatMap(_.split(" "))
      .filter(_.length >= 10)
      .map(_ => longWordValue)
      .reduceByWindow(
        _ + _,
        Seconds(30),
        Seconds(10),
      )


  /**
    * 4) .reduceByKeyAndWindow()
    */
  def reduceByKeyAndWindowValueStream =
    socketStream
      .flatMap(_.split(" "))
      .map { word =>
        ("total value:", if (word.length >= 10) 2 else 0)
      }
      .reduceByKeyAndWindow(
        (a, b) => a + b,
        (a, b) => a - b,
        Seconds(30),
        Seconds(10),
      )


  /**
    * Daniel's Solution
    */
  def reduceByKeyAndWindowValueStreamV2 =
    socketStream
      .flatMap(_.split(" "))
      .filter(_.length >= 10)
      .map { word =>
        if (word.length >= 10) ("expensive", longWordValue)
        else ("cheap", 0)
      }
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(30),
        Seconds(10),
      )


  def main(args: Array[String]): Unit = {
    //    val stream = windowValueStream
    //    val stream = countByWindowValueStream
    //    val stream = reduceByWindowValueStream
    val stream = reduceByKeyAndWindowValueStream

    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
