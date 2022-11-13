package section6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.io.PrintStream
import java.net.ServerSocket
import scala.concurrent.duration._
import java.sql.Timestamp

object Watermarks {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 6.3 - Late Data With Watermarks")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * 2 Second Watermark
    *
    * - For each batch takes the latest event and ignores events earlier than 2 seconds
    * - A window will only be considered until the watermark surpasses the window's end
    * - An element/row/record will be considered if after the watermark
    * - Spark only prints windows older that the watermark (no new data possible)
    *
    * - Input format (TIMESTAMP,VALUE): 3000,blue
    */
  def testWatermark() = {
    val eventsDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        val timestamp = new Timestamp(tokens(0).toLong)
        val value = tokens(1)

        (timestamp, value)
      }
      .toDF("created", "color")


    val watermarkedDF = eventsDF
      .withWatermark("created", "2 seconds")
      .groupBy(
        window($"created", "2 seconds"),
        $"color",
      )
      .count()
      .selectExpr("window.*", "color", "count")


    val query = watermarkedDF
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    debugQuery(query)
    query.awaitTermination()
  }


  /**
    * Inspect the state of the query
    *
    * - Useful for debugging
    * - Probe streaming values with a monitor thread
    */
  def debugQuery(query: StreamingQuery) = {
    new Thread(
      () => {
        (1 to 100).foreach { i =>
          Thread.sleep(1000)

          val queryEventTime =
            if (query.lastProgress == null) "[]"
            else query.lastProgress.eventTime.toString

          println(s"DEBUG: $i: $queryEventTime")
        }
      }
    ).start()
  }


  def main(args: Array[String]): Unit = {
    testWatermark()
  }
}

object DataSender {
  val serverSocket = new ServerSocket(12345)
  /**
    * Connect to socket
    */
  val socket = serverSocket.accept()
  val printer = new PrintStream(socket.getOutputStream)

  println("DEBUG: Socket accepted")

  def example1() = {
    Thread.sleep(7000)
    printer.println("7000,blue")

    Thread.sleep(1000)
    printer.println("8000,green")

    Thread.sleep(4000)
    printer.println("14000,blue")

    Thread.sleep(1000)
    printer.println("9000,red") // Discarded

    Thread.sleep(3000)
    printer.println("15000,red")
    printer.println("8000,blue") // Discarded

    Thread.sleep(1000)
    printer.println("13000,green")

    Thread.sleep(500)
    printer.println("21000,green")

    Thread.sleep(3000)
    printer.println("4000,purple") // Discarded

    Thread.sleep(2000)
    printer.println("17000,green")
  }


  def example2() = {
    printer.println("5000,red")
    printer.println("5000,green")
    printer.println("4000,blue")

    Thread.sleep(7000)
    printer.println("1000,yellow") // Discarded
    printer.println("2000,cyan")
    printer.println("3000,magenta")
    printer.println("5000,black")

    Thread.sleep(3000)
    printer.println("10000,pink")
  }


  def example3() = {
    Thread.sleep(2000)
    printer.println("9000,blue")

    Thread.sleep(3000)
    printer.println("2000,green")
    printer.println("1000,blue")
    printer.println("8000,red")

    Thread.sleep(2000)
    printer.println("5000,red") // Discarded
    printer.println("18000,blue")

    Thread.sleep(1000)
    printer.println("2000,green") // Discarded

    Thread.sleep(2000)
    printer.println("30000,purple")
    printer.println("10000,pink")
  }


  def main(args: Array[String]): Unit = {
    //    example1()
    //    example2()
    example3()
  }
}