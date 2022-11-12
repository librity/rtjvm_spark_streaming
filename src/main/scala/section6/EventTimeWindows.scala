package section6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.{buildJsonPath, inspect, readJson}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object EventTimeWindows {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 6.1 - Event Time Windows")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Read Purchases from a socket
    */
  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType),
  ))

  def readPurchasesFromSocket() = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .select(from_json($"value", onlinePurchaseSchema) as "purchase")
    .selectExpr("purchase.*")


  /**
    * window() Returns a struct colum with {start, end}
    */
  def aggregatePurchasesBySlidingWindow() = {
    val purchasesDF = readPurchasesFromSocket()

    val windowByDay = purchasesDF
      .groupBy(
        window($"time", "1 day", "1 hour") as "time"
      )
      .agg(sum("quantity") as "total_quantity")

    windowByDay
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    aggregatePurchasesBySlidingWindow()
  }
}
