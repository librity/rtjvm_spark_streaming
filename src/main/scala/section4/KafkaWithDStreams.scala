package section4

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

object KafkaWithDStreams {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.1 - Kafka With DStreams")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")

  val ssc = new StreamingContext(sc, Seconds(1))

  /**
    * Configure Kafka
    *
    * - Multiple servers: "localhost:9092, server2:212..."
    * - ".serializer": send data to Kafka
    * - ".deserializers": receive data from Kafka
    * - "auto.offset.reset": if spark crashes, the offset for the given topic will reset to latest
    * - false.asInstanceOf[Object]: false is not a Java Object
    */
  val kafkaConfig: Map[String, Object] = Map(
    "bootstrap.servers" -> "localhost:9092",

    "key.serializer" -> classOf[StringSerializer],
    "value.serializer" -> classOf[StringSerializer],

    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],

    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false.asInstanceOf[Object],
  )
  val targetTopic = "rockthejvm"


  /**
    * LocationStrategies:
    * - .PreferConsistent: distribute partitions evenly across the Spark cluster
    * - .PreferBrokers: use if Spark nodes are also Kafka brokers
    * - .PreferFixed(): Manually set preferred Spark nodes
    *
    * ConsumerStrategies:
    * - .Subscribe: subscribe to a fixed number of topics
    * - .Subscribe[String, String]: key and value are both strings
    * - .SubscribePattern: can supply a RegEx to automatically subscribe to topics
    * - .Assign: allows specifying offsets and partitions per topic (advanced)
    * - "group.id": every stream need a group id to satisfy Kafka cash requirements
    */
  def readFromKafka() = {
    val topics = Array(targetTopic)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils
        .createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies
            .Subscribe[String, String](
              topics,
              kafkaConfig + ("group.id" -> "group1")
            ),
        )

    val processedStream = kafkaDStream
      .map { record => (record.key(), record.value()) }
    processedStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def writeToKafka(): Unit = {
    val inputData = ssc
      .socketTextStream("localhost", 12345)

    val processedData = inputData
      .map(_.toUpperCase())
    processedData.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>

        /**
          * Inside this lambda the code runs in a single executor (process)
          */

        val kafkaHashMap = new util.HashMap[String, Object]()
        kafkaConfig.foreach { pair =>
          kafkaHashMap.put(pair._1, pair._2)
        }

        /**
          * Producer can insert records into the Kafka topics
          * Available on this executor only:
          * executors can't (and shouldn't) share producers
          */
        val producer = new KafkaProducer[String, String](kafkaHashMap)

        partition.foreach { value =>
          val message = new ProducerRecord[String, String](
            targetTopic,
            null,
            value,
          )

          /**
            * Feed the message to the Kafka topic
            */
          producer.send(message)
        }

        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    /**
      * Start a Kafka console producer (see notes.md)
      */
    //    readFromKafka()

    /**
      * Start netcat and a Kafka console consumer (see notes.md)
      */
    writeToKafka()
  }
}
