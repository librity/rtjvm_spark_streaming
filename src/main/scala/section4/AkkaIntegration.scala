package section4

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import common._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Run ReceiverSystem before AkkaIntegration!!!
  */

object AkkaIntegration {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 4.4 - Akka Integration")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * - We will use .forEachBatch()
    * - Receiving system is on another JVM (Akka Remoting)
    */


  def writeCarsToAkka() = {
    val carsDF = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))

    val carsDS = carsDF.as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch
          .foreachPartition { cars: Iterator[Car] =>


            /**
              * This code runs in a single executor (process)
              *
              * - ActorSystems are not serializable
              */
            val system = ActorSystem(
              s"SourceSystem$batchId",
              ConfigFactory
                .load("akkaconfig/remoteActors"),
            )
            val entryPoint = system
              .actorSelection(
                "akka://ReceiverSystem@localhost:2552/user/entrypoint"
              )

            /**
              * Send all the data to the Akka Entry Point on a record by record basis.
              */
            cars.foreach(car => entryPoint ! car)


            /**
              * We can integrate basically anything with .foreachBatch():
              * this could hit a web API, Elastic Search, another cluster, etc.
              *
              * The basic structure will look like this.
              */


          }
      }
      .start()
      .awaitTermination()


  }


  def main(args: Array[String]): Unit = {
    writeCarsToAkka()
  }


}


object ReceiverSystem {
  implicit val actorSystem = ActorSystem(
    "ReceiverSystem",
    ConfigFactory
      .load("akkaconfig/remoteActors")
      .getConfig("remoteSystem"),
  )
  implicit val actorMaterializer = ActorMaterializer()


  /**
    * Destination
    *
    * - End beneficiary to the data sent by Spark
    * - Simple logger in this case
    */
  class Destination extends Actor with ActorLogging {
    override def receive: Receive = {
      case message => log.info(message.toString)
    }
  }


  /**
    * Proxy
    *
    * - Forwards the messages to the destination actor
    * - .!() is the tell method
    * - Props: instantiate actors
    */
  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive: Receive = {
      case message =>
        log.info(s"Received '${message.toString}'")

        destination ! message
    }
  }

  /**
    * Good Practice
    */
  object EntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }


  def namedActorDemo() = {
    /**
      * Create Proxy and Destination actors:
      *
      * - We separate these in case we can't identify the destination actor by name
      */

    val destination = actorSystem.actorOf(
      Props[Destination],
      "destination"
    )

    val entryPoint = actorSystem.actorOf(
      EntryPoint.props(destination),
      "entrypoint",
    )
  }


  def unamedActorDemo() = {
    val source = Source.actorRef[Car](
      bufferSize = 10,
      overflowStrategy = OverflowStrategy.dropHead,
    )
    val sink = Sink.foreach(println)
    val runnableGraph = source.to(sink)
    val actorRef: ActorRef = runnableGraph.run()


    /**
      * In this case we cannot send data to destination actor directly,
      * so we must use the proxy.
      */
    val entryPoint = actorSystem.actorOf(
      EntryPoint.props(actorRef),
      "entrypoint",
    )
  }


  def main(args: Array[String]): Unit = {
    //    runProxyDestinationDemo()
    unamedActorDemo()
  }
}