package section4

import common._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object JDBCIntegration {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 4.3 - JDBC Integration")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Configure Postgres connector
    */
  val postgresConfig = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker",
  )


  def writeStreamToPostgres() = {
    val carsDF = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))

    val carsDS = carsDF.as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>


        /**
          * Each executor can control each batch
          * The batch is a static Data Ser or Data Frame,
          * and we can write it to postgres as we would any other!
          */


        batch
          .write
          .format("jdbc")
          .mode(SaveMode.Overwrite)
          .options(postgresConfig)
          .option("dbtable", "public.cars")
          .save()


      }
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    /**
      * Start the Postgres Docker container
      */
    writeStreamToPostgres()
  }
}
