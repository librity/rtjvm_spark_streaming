package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status


object TweetsReader {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - Tweets Reader")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(5))

  /**
    * Read Tweets as a DStream.
    */

  def readTweets() = {
    val twitterStream: DStream[Status] = ssc
      .receiverStream(new TwitterReceiver)

    val tweets: DStream[String] = twitterStream
      .map { status =>
        val username = status.getUser.getName
        val followers = status.getUser.getFollowersCount
        val text = status.getText

        s"User $username ($followers followers) says: '$text'"
      }

    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readTweets()
  }
}
