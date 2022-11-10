package section4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import common.{buildDataPath, buildJsonPath, carsSchema, inspect, readJson}

object KafkaIntegration {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 4.1 - Kafka Integration")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def readFromKafka() = {
    /**
      * Subscribe to multiple topics with commas:
      * .option("subscribe", "football, golf, rugby")
      *
      * "kafka.bootstrap.servers" and "subscribe" are required
      */
    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()

    kafkaDF
      .select(
        $"topic",
        expr("cast(value as string) AS value"),
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }


  def writeToKafka() = {
    val carsDF = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))

    /**
      * Kafka Data Frames must have "key" and "value" columns
      */
    val carsKafkaDF = carsDF
      .selectExpr(
        "upper(Name) as key",
        "Name as value",
      )

    /**
      * "checkpointLocation", "kafka.bootstrap.servers" and "topic" are required
      *
      * Writing stream to Kafka requires checkpoints:
      * - This is how Spark keeps track of which data it has already sent
      * - Delete the checkpoints directory to before every test
      */
    carsKafkaDF
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    /**
      * Start a Kafka console producer (see notes.md)
      */
    //    readFromKafka()


    /**
      * Start a Kafka console consumer (see notes.md)
      */
    writeToKafka()
  }
}
