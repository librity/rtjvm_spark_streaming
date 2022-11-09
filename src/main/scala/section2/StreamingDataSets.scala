package section2

import common.{Car, carsSchema, getSocketConfig}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDataSets {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 2.4 - Streaming Data Sets")
    // "local[*]": Run Spark locally with as many worker threads as logical cores on your machine.
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def readCars: Dataset[Car] = {
    /**
      * We can also pass the encoder explicitly
      */
    val carEncoder = Encoders.product[Car]

    spark
      .readStream
      .format("socket")
      .options(getSocketConfig(12345))
      .load()
      .select(
        from_json($"value", carsSchema) as "cars"
      )
      .selectExpr("cars.*")
      .as[Car](carEncoder)
  }

  def showCarNames = {
    val carsDS = readCars

    /**
      * Lose type information
      */
    val carNamesDF: DataFrame = carsDS.select('Name)

    /**
      * Maintai type information
      */
    val carNamesDS: Dataset[String] = carsDS.map(_.Name)

    carNamesDS
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /**
      * Run "nc -lk 12345" and paste car jsons before running.
      */
    showCarNames
  }
}
