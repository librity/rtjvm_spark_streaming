package section3

import common.{Person, buildDataPath, buildJsonPath}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.sql.Date
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}

object DStreamTransformations {


  /**
    * Boilerplate
    */
  val spark = SparkSession
    .builder()
    .appName("Lesson 3.2 - DStream Transformations")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(1))


  /**
    * people-1m.txt format:
    *
    * id:first_name:middle_name:last_name:gender:date_of_birth:social_security_number:salary
    * 7:Geri:Tambra:Mosby:F:1970-12-19:968-16-4020:38195
    */
  def getPeopleStream: DStream[Person] = ssc
    .socketTextStream("localhost", 12345)
    .map { line =>
      val tokens = line.split(":")

      Person(
        id = tokens(0).toInt,
        firstName = tokens(1),
        middleName = tokens(2),
        lastName = tokens(3),
        gender = tokens(4),
        birthDate = Date.valueOf(tokens(5)),
        ssn = tokens(6),
        salary = tokens(7).toInt,
      )
    }


  /**
    * .map()
    */
  def aesStream = getPeopleStream
    .map { person =>
      val age = Period.between(
        person.birthDate.toLocalDate,
        LocalDate.now(),
      ).getYears

      (person.fullName, age)
    }


  /**
    * .flatMap()
    */
  def namesStream = getPeopleStream
    .flatMap { person =>
      List(person.firstName, person.lastName)
    }


  /**
    * .filter()
    */
  def fatCatsStream = getPeopleStream
    .filter(_.salary > 80000)


  /**
    * .count()
    */
  def countStream = getPeopleStream
    .count()


  /**
    * .countByValue()
    *
    * - Works per batch.
    */
  def nameCountStream = getPeopleStream
    .map(_.firstName)
    .countByValue()


  /**
    * .reduceByKey()
    *
    * - Works on a DStream of tuples
    * - Works per batch
    */
  def mapReducedNamesStream = getPeopleStream
    // .map(_.firstName)
    // .map((_, 1))
    .map(person => (person.firstName, 1))
    // .reduceByKey((a, b)=> a + b)
    .reduceByKey(_ + _)


  /**
    * .foreachRDD()
    *
    * - Receive all the RDDs of a stream and do whatever we want with them
    */
  def saveStreamToJson(): Unit = getPeopleStream
    .foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val personEncoder = Encoders.product[Person]
        val batchDataSet = spark.createDataset(rdd)(personEncoder)

        val dir = new File(buildDataPath("people"))
        val fileCount = dir.listFiles().length
        val path = buildDataPath(s"people/people_$fileCount.json")

        batchDataSet.write.json(path)
      }
    }

  def main(args: Array[String]): Unit = {
    /**
      * $ cd src/main/resources/data/people-1m
      * $ cat people-1m.txt | nc -lk 12345
      */

    //    val stream =     getPeopleStream
    //    val stream =     agesStream
    //    val stream =     namesStream
    //    val stream =     fatCatsStream
    //    val stream =     countStream
    //    val stream =     nameCountStream
    //    val stream = mapReducedNamesStream
    //    stream.print()

    saveStreamToJson()

    ssc.start()
    ssc.awaitTermination()
  }
}
