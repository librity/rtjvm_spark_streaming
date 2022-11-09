package section2

import common.{Car, carsSchema, getSocketConfig}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDataSetsExercise {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 2.4 - Streaming Data Sets")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def readCarsDF: Dataset[Car] = {
    spark
      .readStream
      .format("socket")
      .options(getSocketConfig(12345))
      .load()
      .select(
        from_json($"value", carsSchema) as "cars"
      )
      .selectExpr("cars.*")
      .as[Car]
  }

  /**
    * Exercises
    *
    * 1. Count how many powerful cars we have in the streamed Data Set (HP > 140)
    */

  def streamHotRodsCount = {
    val hotRodsCount = readCarsDF
      .filter(_.Horsepower.getOrElse(0L) > 140L)
      .select(count("*") as "hot_rods_count")

    hotRodsCount
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * 2. Print average HP of the entire Data Set (Complete Output Mode)
    */

  def streamAverageHP = {
    val averageHP = readCarsDF
      .agg(avg("Horsepower") as "average_hp")

    /**
      * Daniel's solution
      */
    val averageHPV2 = readCarsDF
      .select(avg("Horsepower") as "average_hp")

    averageHP
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
    * 3. Count cars by Origin
    */

  def streamByOrigin = {
    val byOrigin = readCarsDF
      .where($"Origin".isNotNull)
      .groupBy("Origin")
      .agg(
        count("Origin") as "count"
      )


    /**
      * Daniel's solution
      */
    val byOriginV2 = readCarsDF
      .groupBy("Origin")
      .count()
    val byOriginV3 = readCarsDF
      .groupByKey(_.Origin)
      .count()


    byOriginV3
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /**
      * Run "nc -lk 12345" and paste car jsons before running.
      */

    // streamHotRodsCount
    // streamAverageHP
    // streamByOrigin
  }
}
