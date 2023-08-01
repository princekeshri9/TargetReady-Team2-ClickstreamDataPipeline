package com.target_ready.data.pipeline.producer

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import scala.io.Source

object Producer {
  def main(args: Array[String]): Unit = {

    /** Setting the Configuration properties for kafka producer */
    val config: Properties = new Properties()
    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    /** Creating Kafka Producer Object */
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    val fileName = "C://target_ready//Phase 2//TargetReady-Team2-ClickStreamDatatPipeline//data//Input//item/item_data.csv"

    val topicName = "clickStream"


    /** The LOOP reads every line of .csv file
     * (EXCEPT HEADER) and send it to the Topic
     * with its Corresponding key */

    for (line <- Source.fromFile(fileName).getLines().drop(1)) {  /** Droping the Header */

      val key = line.split(",") {0} /** Extracting key from every line */

      /** Preparing the data */
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, line)

      /** Send to topic */
      producer.send(record)

    }

    /** Flushing the Producer (blocking it until previous messaged
     * have been delivered effectively, to make it synchronous) */
    producer.flush()

    /** Closing the Producer */
    producer.close()

  }
}


























////=================================================================================
//
//import org.apache.spark.sql.SparkSession
////
////import scala.io.Source
////import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//object Producer {
//
//  def main(args: Array[String]): Unit = {
//
//        val spark: SparkSession = SparkSession.builder()
//          .master("local[3]")
//          .appName("Spark1")
//          .getOrCreate()
//
//        val schema = StructType(
//          List(
//            StructField("item_id", StringType, true),
//            StructField("item_price", StringType, true),
//            StructField("product_type", StringType, true),
//            StructField("department_name", StringType, true)
//          )
//        )
//
//        val df = spark.readStream.schema(schema).csv("C:/target_ready/Phase 2/CLICKSTREAM-DATA-PIPELINE/data/Input/item")
//
//    df.writeStream
//      .format("console")
//      .outputMode("append")
//      .option("truncate",false)
//      .option("newRows",30)
//      .start()
//      .awaitTermination()
//
//  }
//}