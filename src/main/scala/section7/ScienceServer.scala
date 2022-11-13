package section7

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Directives._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringSerializer, LongSerializer}

import java.util.Properties
import scala.io.Source

object ScienceServer {
  /**
    * - Akka Actor System: Thread pool on which actors run
    * - Akka Actor Materializer: What Akka uses to handle the Web Server's stream
    */
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val kafkaTopic = "science"
  val kafkaBootstrapServer = "localhost:9092"


  def getKafkaProducer() = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "My Kafka Science Producer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    new KafkaProducer[Long, String](props)
  }

  /**
    * Configure GET Request on root path to return whackamole.html
    */
  def getRoute(producer: KafkaProducer[Long, String]) = {
    pathEndOrSingleSlash {
      get {
        complete(
          HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            Source.fromFile("src/main/html/whackamole.html").getLines().mkString(""),
          )
        )
      }
    } ~ path("api" / "report") {
      (parameter("sessionId".as[String])
        & parameter("time".as[Long])) {
        (sessionId: String, time: Long) =>
          println(s"{ sessionId: $sessionId, time: $time }")

          /**
            * Create and send Kafka Producer Record
            *
            * - producer.flush(): Sends all buffered records
            */
          val record = new ProducerRecord[Long, String](
            kafkaTopic,
            0,
            s"$sessionId,$time")
          producer.send(record)
          producer.flush()

          complete(StatusCodes.OK)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val kafkaProducer = getKafkaProducer()

    val bindingFuture = Http().
      bindAndHandle(
        getRoute(kafkaProducer),
        "localhost",
        9988)

    /**
      * Cleanup
      */
    import scala.concurrent.ExecutionContext.Implicits.global
    bindingFuture.foreach { binding =>
      binding.whenTerminated.onComplete(_ => kafkaProducer.close())
    }
  }
}
