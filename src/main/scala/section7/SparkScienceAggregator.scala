package section7

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import common.{buildJsonPath, inspect, readJson}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

object SparkScienceAggregator {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 7.2 - Spark Science Aggregator")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  case class UserResponse(sessionId: String, clickInterval: Long)

  case class UserAverageResponse(sessionId: String, avgInterval: Double)

  def readUserResponses(): Dataset[UserResponse] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "science")
    .load()
    .select("value")
    .as[String]
    .map { line =>
      val tokens = line.split(",")

      UserResponse(tokens(0), tokens(1).toLong)
    }


  def logUserResponses() = {
    readUserResponses()
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  /**
    * Aggregate the ROLLING average response time over the past 10 clicks
    *
    * - .flatMapGroupsWithState() is way better than Spark SQL for this
    * - Sliding windows account for time, not for last n records
    */
  def getAverageIntervalTime(clicks: Int) = {
    readUserResponses()
      .groupByKey(_.sessionId)
      .flatMapGroupsWithState(
        OutputMode.Append(),
        GroupStateTimeout.NoTimeout(),
      )(updateUserIntervalTime(clicks))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  /**
    * Compute the average response time of the last 10 clicks
    *
    * @param clicks    The number of clicks to average over.
    *                  Pass an outer parameter to the callback!
    * @param sessionId The key of the group we're updating.
    * @param group     The next batch of that group.
    * @param state     The previous data, a List of UserResonse in this case.
    * @return The new Average Response of that user
    *
    *
    *         Example:
    *         updateUserIntervalTime(3)("sessionId", [100, 200, 300, 400, 500, 600], Empty) => Iterator(200, 300, 400, 500)
    *
    *         100 -> state = [100]
    *         200 -> state = [100, 200]
    *         300 -> state = [100, 200, 300] -> first average 200
    *         400 -> state = [200, 300, 400] -> first average 300
    *         500 -> state = [300, 400, 500] -> first average 400
    *         600 -> state = [400, 500, 600] -> first average 500
    *
    *         Iterator = [200, 300, 400, 500]
    */
  def updateUserIntervalTime
  (clicks: Int)
  (
    sessionId: String,
    group: Iterator[UserResponse],
    state: GroupState[List[UserResponse]]
  ): Iterator[UserAverageResponse] = {
    group.flatMap { record =>
      val lastWindow =
        if (state.exists) state.get
        else List()
      val windowLength = lastWindow.length

      /**
        * Pop the last record anc/or append
        */
      val newWindow =
        if (windowLength >= clicks) lastWindow.tail :+ record
        else lastWindow :+ record

      /**
        * For Spark to give us access to the state on the next batch
        */
      state.update(newWindow)
      if (newWindow.length >= clicks) {
        val newAverage = newWindow.map(_.clickInterval).sum * 1.0 / clicks

        Iterator(UserAverageResponse(sessionId, newAverage))
      } else {
        Iterator()
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //    logUserResponses()
    getAverageIntervalTime(10)
  }
}
