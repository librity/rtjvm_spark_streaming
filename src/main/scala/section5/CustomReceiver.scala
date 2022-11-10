package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.{buildJsonPath, inspect, readJson}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.Socket
import scala.concurrent.{Future, Promise}
import scala.io.Source


/**
  * Receiver Class
  *
  * - Reads Data
  * - Functional implementation (usually declarative)
  * - Use StorageLevel.MEMORY_AND_DISK_2 in production for fault tolerance
  */
class CustomSocketReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future


  /**
    * .onStart()
    *
    * - Called when receiver starts receiving data
    * - Source.fromInputStream(): Functional source
    * - store(): From Receiver[](), exposes line String to Spark
    * - Called asynchronously, runs on a thread (Future)
    */
  override def onStart(): Unit = {
    val socket = new Socket(host, port)


    Future {
      Source
        .fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line))
    }

    socketPromise.success(socket)
  }


  /**
    * .onStop()
    *
    * - Called when receiver stops the DStream
    * - Called asynchronously, runs on a thread (Future)
    * - Close sockets running on each executor to release its resources
    */
  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}


object CustomReceiver {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - Custom Receiver")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(1))


  /**
    * Read from a network socket with a Custom Receiver and echo it
    */
  def echoCustomReceiver() = {
    val dataSteam: DStream[String] = ssc
      .receiverStream(new CustomSocketReceiver("localhost", 12345))

    dataSteam.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    echoCustomReceiver()
  }
}
