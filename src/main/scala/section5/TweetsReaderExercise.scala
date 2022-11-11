package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import twitter4j.Status


object TweetsReaderExercise {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.1 - Tweets Reader Exercise")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(1))
  ssc.checkpoint("checkpoints")


  /**
    * Exercise
    *
    */
  def getTwitterStream(): DStream[Status] = ssc
    .receiverStream(new TwitterAPIv1Receiver)


  /**
    * 1. Compute average length of tweets in the past 5 seconds every 5 seconds
    */
  def countAverageTweetLength(): DStream[Double] = getTwitterStream()
    .map(status => (status.getText.length, 1))
    .reduceByWindow(
      (a, b) => (a._1 + b._1, a._2 + b._2),
      Seconds(5),
      Seconds(5),
    )
    .map { results =>
      val lengthSum = results._1
      val tweetCount = results._1

      lengthSum * 1.0 / tweetCount
    }


  /**
    * 2. Compute most popular hashtags during a 1 minute window, every 10 seconds
    *
    * .transform(): like .map() for DStreams
    */
  def printPopularHashtags() = getTwitterStream()
    .flatMap(_.getText.split(" "))
    .filter(_.startsWith("#"))
    .map(hashtags => (hashtags, 1))
    .reduceByKeyAndWindow(
      _ + _,
      _ - _,
      Minutes(1),
      Seconds(10),
    )
    // .transform(rdd => rdd.sortBy( - _._2))
    .transform(rdd => rdd.sortBy(tuple => -tuple._2))


  def main(args: Array[String]): Unit = {
    val stream = countAverageTweetLength()
    //    val stream = printPopularHashtags()


    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
