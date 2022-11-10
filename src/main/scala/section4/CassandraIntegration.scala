package section4

import com.datastax.spark.connector.cql.CassandraConnector
import common._
import org.apache.spark.sql.{Dataset, ForeachWriter, SaveMode, SparkSession}
import org.apache.spark.sql.cassandra._

object CassandraIntegration {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 4.5 - Cassandra Integration")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    *
    */

  def writeStreamToCassandraInBatches() = {
    val carsDS = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))
      .as[Car]


    carsDS.writeStream.foreachBatch { (batch: Dataset[Car], _: Long) =>

      /**
        * Save batch to Cassandra in a single transaction (write)
        */
      batch
        .select($"Name", $"Horsepower")
        .write
        .cassandraFormat("cars", "public")
        .mode(SaveMode.Append)
        .save()

    }
      .start()
      .awaitTermination()


  }


  class CassandraCarWriter extends ForeachWriter[Car] {
    /**
      * On every batch, on every partition and on every epoch (chunk of data) Spark will:
      * 1. Call .open() and skip epoch if it returns false
      * 2. For each entry in this epoch, call .process()
      * 3. Call .close() at the end of the epoch or before if an error was thrown
      */

    val keyspace = "public"
    val table = "cars"
    val connector = CassandraConnector(sc.getConf)


    override def open(partitionId: Long, epochId: Long): Boolean = {
      println(s"Opened Cassandra connection for epoch $epochId.")

      true
    }

    override def process(car: Car): Unit = {
      connector.withSessionDo { session =>
        session.execute(
          s"""
             |INSERT INTO $keyspace.$table("Name", "Horsepower")
             |    values('${car.Name}', ${car.Horsepower.orNull})
             |""".stripMargin)
      }
    }

    override def close(errorOrNull: Throwable): Unit = {
      println(s"Closed Cassandra connection")
    }
  }


  /**
    * .foreach(writer)
    *
    * - Better performance than with .foreachBatch()
    */
  def writeStreamToCassandra() = {
    val carsDS = spark
      .readStream
      .schema(carsSchema)
      .json(buildDataPath("cars"))
      .as[Car]


    carsDS
      .writeStream
      .foreach(new CassandraCarWriter)
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    //    writeStreamToCassandraInBatches()
    writeStreamToCassandra()
  }
}
