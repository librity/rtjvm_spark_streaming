
package section2

import common.{defaultSocketConfig, getDataFrameStream, getSocketConfig, inspect, readJson}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object StreamingJoins {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 2.3 - Streaming Joins")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Read Data Frames
    */
  val guitarists = readJson(spark, "guitarPlayers")
  val guitars = readJson(spark, "guitars")
  val bands = readJson(spark, "bands")

  val guitaristSchema = guitarists.schema
  val bandSchema = bands.schema


  /**
    * Joins
    */

  val joinCondition = guitarists.col("band") === bands.col("id")
  val guitaristBands = guitarists
    .join(bands, joinCondition)

  def joinStreamWithStatic = {
    /**
      * A Data Frame with a single "value" column of type String.
      */
    val rawStreamedBands = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()

    val streamedBands = rawStreamedBands
      .select(from_json($"value", bandSchema) as "band")
      .selectExpr(
        "band.id AS id",
        "band.name AS name",
        "band.hometown AS hometown",
        "band.year AS year"
      )

    /**
      * Join happens per batch.
      */
    val streamedGuitaristBands = streamedBands
      .join(
        guitarists,
        guitarists.col("band") === streamedBands.col("id"),
        "inner",
      )

    /**
      * .join() Restrictions:
      *
      * - streamedDF.join(staticDF): Right Outer, Full Outer and Right Semi are not permitted.
      * - staticDF.join(streamedDF): Left Outer, Full Outer, Left Semi are not permitted.
      *
      * Spark does not allow unbounded accumulation of data.
      */
    streamedGuitaristBands
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def joinStreamWithStream = {
    val streamedBands = getDataFrameStream(spark, 12345)
      .select(from_json($"value", bandSchema) as "band")
      .selectExpr(
        "band.id AS id",
        "band.name AS name",
        "band.hometown AS hometown",
        "band.year AS year"
      )

    val streamedGuitarists = getDataFrameStream(spark, 12346)
      .select(from_json($"value", guitaristSchema) as "guitarists")
      .selectExpr(
        "guitarists.id AS id",
        "guitarists.name AS name",
        "guitarists.guitars AS guitars",
        "guitarists.band AS band"
      )

    val streamJoin = streamedBands
      .join(
        streamedGuitarists,
        streamedBands.col("id") === streamedGuitarists.col("band"),
        "inner",
      )

    /**
      * streamedDF.join(streamedDF):
      *
      * - Only supports Append Output Mode.
      * - Inner joins are supported
      * - Left and Right outer joins are supported WITH "WATERMARKS"
      * - Full outer joins are not supported
      * - We can enhance performance with "watermarks" (ADVANCED)
      */
    streamJoin
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    //    inspect(guitaristBands)

    /**
      * Run "nc -lk 12345" and paste band jsons before running.
      */
    //    joinStreamWithStatic


    /**
      * Run "nc -lk 12345" and "nc -lk 12346".
      * Paste bands a guitarist jsons into each stream.
      */
    //    joinStreamWithStream

  }
}
