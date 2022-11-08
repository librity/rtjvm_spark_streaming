import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

package object common {
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val stocksSchema = StructType(Array(
    StructField("company", StringType),
    StructField("date", DateType),
    StructField("value", DoubleType)
  ))

  def inspect(dataFrame: DataFrame): Unit = {
    dataFrame.printSchema()
    dataFrame.show()
    println(s"Size: ${dataFrame.count()}")
  }

  def readJson(spark: SparkSession, fileName: String): DataFrame = spark
    .read
    .option("inferSchema", "true")
    .json(buildJsonPath(fileName))

  def buildJsonPath(fileName: String): String =
    s"src/main/resources/data/${fileName}/${fileName}.json"
}
