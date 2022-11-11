package section5

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.config.CookieSpecs
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.net.URISyntaxException

import com.typesafe.config.ConfigFactory

/*
 * Sample code to demonstrate the use of the Sampled Stream endpoint
 */
object TwitterSampledStreamDemo {


  @throws[IOException]
  @throws[URISyntaxException]
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load("twitterAPI");
    val bearerToken = config.getString("oauth.bearerToken")

    if (bearerToken == null) {
      println("There was a problem getting your bearer token. Please make sure you set the TWITTER_BEARER_TOKEN environment variable")
      System.exit(1)
    }
    connectStream(bearerToken)
  }


  /*
   * This method calls the sample stream endpoint and streams Tweets from it
   */
  @throws[IOException]
  @throws[URISyntaxException]
  private def connectStream(bearerToken: String): Unit = {
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

    val reader = new BufferedReader(
      new InputStreamReader(entity.getContent)
    )

    var line = reader.readLine
    while (line != null) {
      println(line)
      line = reader.readLine
    }

  }
}