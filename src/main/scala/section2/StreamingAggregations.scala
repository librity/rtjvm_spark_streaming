package section2

import common.defaultSocketConfig
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 2.2 - Streaming Aggregations")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  def countStreamLines = {
    val lines: DataFrame = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()

    val lineCount = lines
      .selectExpr("count(*) AS line_count")

    /**
      * Append and Update Output Modes are only supported
      * for Aggregations with "watermarks" (more on that later).
      *
      * Aggregations with sorting or distinct are not supported at all:
      * would need to keep the entire unbounded state of the stream in memory.
      */
    lineCount
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def sumNumbersStream = {
    val lines = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()

    val numbers = lines
      .select(
        $"value".cast("integer").as("number")
      )
    val sumNumbers = numbers
      .select(
        sum($"number").as("sum_so_far")
      )

    sumNumbers
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregateNumbersStream(aggFunction: Column => Column) = {
    val lines = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()

    val numbers = lines
      .select(
        $"value".cast("integer").as("number")
      )
    val aggregation = numbers
      .select(
        aggFunction($"number").as("agg_so_far")
      )

    aggregation
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames = {
    val lines = spark
      .readStream
      .format("socket")
      .options(defaultSocketConfig)
      .load()


    val names = lines
      .select(lower($"value") as "name")
      .groupBy("name")
      .count()


    names
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /**
      * Run "nc -lk 12345" and type stuff before running.
      */
    //    streamingCount
    //    sumNumbersStream

    //    aggregateNumbersStream(avg)
    //    aggregateNumbersStream(sum)
    //    aggregateNumbersStream(min)
    //    aggregateNumbersStream(max)
    //    aggregateNumbersStream(stddev)

    //    groupNames
  }
}
