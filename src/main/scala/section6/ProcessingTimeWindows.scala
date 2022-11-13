package section6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindows {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 6.2 - Processing Time Windows")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def aggregateByProcessingTime() = {
    val linesLengthByWindowDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select($"value", current_timestamp() as "processing_time")
      .groupBy(
        window($"processing_time", "10 seconds") as "window"
      )
      .agg(
        sum(length($"value")) as "char_count"
      )
      .select(
        $"window" getField "start" as "start",
        $"window" getField "end" as "end",
        $"char_count",
      )



    linesLengthByWindowDF
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    aggregateByProcessingTime()
  }
}
