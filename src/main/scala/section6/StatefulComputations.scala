package section6

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

object StatefulComputations {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 6.5 - Stateful Computations")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  /**
    * Read a stream of social media storage used and aggregate average
    *
    * - Input format (POST_TYPE,COUNT,STORAGE_USED_BYTES): text,5,1024
    */
  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)

  case class SocialPostBulk(postType: String, count: Int, totalStorageUsed: Int)

  case class AveragePostStorage(postType: String, averageStorage: Double)


  def readSocialUpdates() = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .as[String]
    .map { line =>
      val tokens = line.split(",")

      SocialPostRecord(tokens(0), tokens(1).toInt, tokens(2).toInt)
    }

  def getAveragePostStorage() = {
    val socialStream = readSocialUpdates()

    /**
      * What we're used to (bad performance)
      *
      * - Recalculates Aggregation for all groups regardless of the scope of the new data
      */
    val regularSqlAverageByPostType = socialStream
      .groupByKey(_.postType)
      .agg(
        (sum($"count") as "total_count").as[Int],
        (sum($"storageUsed") as "totalStorage").as[Int],
      )
      .selectExpr("key AS postType", "totalStorage / total_count AS averageStorage")


    /**
      * .mapGroupsWithState()
      *
      * - .map() but we can control the aggregation with each batch
      * - Only recalculates Aggregation for new items of a single group (better performance)
      * - A lot more control over the computation than with the SQL API
      * - Append output mode not supported
      *
      * - GroupStateTimeout: Like a watermark, how late to consider incoming data
      * - .NoTimeout(): Take into account all data regardless of event time
      * - The data isn't time independent in this case, and we don't filter by event time
      *
      * - Use .flatMapGroupsWithState() for groups that return multiple outputs
      */
    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .mapGroupsWithState(
        GroupStateTimeout.NoTimeout()
      )(updateAverageStorage)

    //    regularSqlAverageByPostType
    averageByPostType
      .writeStream
      .outputMode("update")
      .foreachBatch { (batch: Dataset[AveragePostStorage], _: Long) =>
        batch.show()
      }
      .start()
      .awaitTermination()
  }

  /**
    * .mapGroupsWithState() callback
    *
    * 1. Extract the initial state
    * 2. Aggregate data for all the items in the group:
    *   - Sum the total count
    *   - Sum the total storage
    *
    * 3. Update the state with the new aggregated data
    * 4. Return a single value of average post storage.
    *
    * @param postType The key by which the grouping was named.
    * @param group    A batch of data associated with the key.
    * @param state    Like an Option, a wrapper over a piece of data.
    *                 Manage this manually.
    * @return A single value calculated for the entire group.
    */
  def updateAverageStorage(
                            postType: String,
                            group: Iterator[SocialPostRecord],
                            state: GroupState[SocialPostBulk]): AveragePostStorage = {
    // 1. Extract the initial state
    val previousBulk =
      if (state.exists) state.get
      else SocialPostBulk(postType, 0, 0)

    // 2. Aggregate data for all the items in the group
    val totalAggregatedData = group
      .foldLeft((0, 0)) { (currentData, record) =>
        val (currentCount, currentStorage) = currentData

        (currentCount + record.count, currentStorage + record.storageUsed)
      }

    // 3. Update the state with the new aggregated data
    val (totalCount, totalStorage) = totalAggregatedData
    val newPostBulk = SocialPostBulk(
      postType = postType,
      count = previousBulk.count + totalCount,
      totalStorageUsed = previousBulk.totalStorageUsed + totalStorage,
    )
    state.update(newPostBulk)

    // 4. Return a single value of average post storage
    AveragePostStorage(
      postType = postType,
      averageStorage = newPostBulk.totalStorageUsed * 1.0 / newPostBulk.count,
    )
  }


  def main(args: Array[String]): Unit = {
    /**
      * Run "nc -lk 12345" and send data before running:
      * - src/main/resources/post_storages.csv
      */
    getAveragePostStorage()
  }
}
