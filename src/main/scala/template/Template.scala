package template

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import common.{buildJsonPath, inspect, readJson}

object Template {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson 1.2 - Spark Recap")
    // Run locally on 2 threads
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def main(args: Array[String]): Unit = {

  }
}
