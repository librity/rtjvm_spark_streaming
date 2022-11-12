package section5

import java.io.{InputStream, BufferedReader, IOException, InputStreamReader}
import java.net.URISyntaxException

import scala.concurrent.{Future, Promise}
import scala.io.Source

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients

import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import spray.json._
import DefaultJsonProtocol._

class TwitterSampledStreamJsonReceiver
  extends Receiver[JsValue](StorageLevel.MEMORY_ONLY) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[InputStream] = Promise[InputStream]()
  val socketFuture = socketPromise.future


  override def onStart(): Unit = {
    val bearerToken = getBearerTokenOrDie
    val stream = connectStreamOrDie(bearerToken)


    Future {
      Source
        .fromInputStream(stream)
        .getLines()
        .foreach(line => store(line.parseJson))
    }

    socketPromise.success(stream)
  }


  override def onStop(): Unit = socketFuture.foreach(stream => stream.close())

  private

  def getBearerTokenOrDie: String = {
    val config = ConfigFactory.load("twitterAPI")
    val bearerToken = config.getString("oauth.bearerToken")

    if (bearerToken == null) {
      println("ERROR: Couldn't load Bearer Token from 'src/main/resources/twitterAPI.conf'")
      System.exit(1)
    }

    bearerToken
  }

  def connectStreamOrDie(bearerToken: String): InputStream = {
    val httpClient = HttpClients
      .custom
      .setDefaultRequestConfig(
        RequestConfig
          .custom
          .setCookieSpec(CookieSpecs.STANDARD)
          .build
      ).build

    val uri = "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=created_at&expansions=author_id&user.fields=created_at"
    val uriBuilder = new URIBuilder(uri)
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (entity == null) {
      println("ERROR: Null HTTP Response entity.")
      System.exit(1)
    }

    entity.getContent
  }


}


object TweetsJsonReaderV2 {
  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - Twitter API v2 JSON Stream Reader")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(1))


  def echoTwitterReceiver() = {
    val dataSteam = ssc
      .receiverStream(new TwitterSampledStreamJsonReceiver())
      .map { tweetJson =>
        tweetJson.asJsObject.getFields("data") match {


          case Seq(obj: JsObject) =>
            obj.getFields("text") match {
              case Seq(JsString(text)) => text
              case unrecognized => deserializationError(s"JSON serialization error: $unrecognized")
            }
          case unrecognized => deserializationError(s"JSON serialization error: $unrecognized")


        }
      }

    dataSteam.print()

    ssc.start()
    ssc.awaitTermination()
  }


  @throws[IOException]
  @throws[URISyntaxException]
  def main(args: Array[String]): Unit = {
    echoTwitterReceiver()
  }
}

