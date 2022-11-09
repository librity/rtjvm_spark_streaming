package section4

import common.{buildDataPath, carsSchema}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaIntegrationExercise {
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


  /**
    * Exercise
    *
    * 1. Write the entire cars Data Frame to Kafka as JSON
    * - Use struct columns and to_json()
    */
  def writeCarsJsonToKafka() = {
    val cars = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))

    val kafkaCars = cars
      .select(
        $"Name" as "key",
        to_json(struct($"*")) as "value",
      )

    kafkaCars
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }


  /**
    * Daniel's Solution
    */
  def writeCarsJsonToKafkaV2() = {
    val cars = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))

    val kafkaCars = cars
      .select(
        $"Name" as "key",
        to_json(struct(
          $"Name",
          $"Horsepower",
          $"Origin",
        )) cast "String" as "value",
      )

    kafkaCars
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
      * Start a Kafka console consumer (see notes.md)
      */
    //    writeCarsJsonToKafka()
    writeCarsJsonToKafkaV2()
  }
}
