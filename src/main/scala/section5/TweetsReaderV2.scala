package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.{buildJsonPath, inspect, readJson}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.net.Socket
import java.io.InputStream
import scala.concurrent.{Future, Promise}
import scala.io.Source

import org.apache.http.{HttpEntity, HttpResponse}
import org.apache.http.client.HttpClient
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.URISyntaxException

import com.typesafe.config.ConfigFactory


class TwitterSampledStreamReceiver
  extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[InputStream] = Promise[InputStream]()
  val socketFuture = socketPromise.future


  override def onStart(): Unit = {
    val beareToken = getBearerTokenOrDie()
    val stream = connectStream(beareToken)


    Future {
      Source
        .fromInputStream(stream)
        .getLines()
        .foreach(line => store(line))
    }

    socketPromise.success(stream)
  }


  override def onStop(): Unit = socketFuture.foreach(stream => stream.close())

  private

  def getBearerTokenOrDie() = {
    val config = ConfigFactory.load("twitterAPI")
    val bearerToken = config.getString("oauth.bearerToken")

    if (bearerToken == null) {
      println("There was a problem getting your bearer token. Please make sure you set the TWITTER_BEARER_TOKEN environment variable")
      System.exit(1)
    }

    bearerToken
  }

  def connectStream(bearerToken: String) = {
    val httpClient = HttpClients
      .custom
      .setDefaultRequestConfig(
        RequestConfig
          .custom
          .setCookieSpec(CookieSpecs.STANDARD)
          .build
      ).build

    val uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/sample/stream")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken))

    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    if (entity == null) {
      println("ERROR: Null entity.")
      System.exit(1)
    }

    entity.getContent
  }


}


object TweetsReaderV2 {
  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - Custom Receiver")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(1))


  def echoTwitterReceiver() = {
    val dataSteam: DStream[String] = ssc
      .receiverStream(new TwitterSampledStreamReceiver())

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

