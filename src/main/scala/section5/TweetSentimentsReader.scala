package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status


object TweetSentimentsReader {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.5 - Tweet Sentiments Reader")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(5))

  /**
    * Read Tweets as a DStream.
    */

  def readTweetSentiments() = {
    val twitterStream: DStream[Status] = ssc
      .receiverStream(new TwitterAPIv1Receiver)

    val tweets: DStream[String] = twitterStream
      .map { status =>
        val username = status.getUser.getName
        val followers = status.getUser.getFollowersCount
        val text = status.getText
        val sentiment = SentimentAnalysis.detectSentiment(text)

        s"User $username ($followers followers) says $sentiment: '$text'"
      }

    tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readTweetSentiments()
  }
}
