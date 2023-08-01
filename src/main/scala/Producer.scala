//import java.util.Properties
//
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
//import org.apache.kafka.common.serialization.StringSerializer
//
//import scala.io.Source
//
//object Producer {
//  def main(args: Array[String]): Unit = {
//    val config: Properties = new Properties()
//    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Enter your own bootstrap server IP:PORT
//    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
//
//    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)
//
//    // Enter your file name with path here
//    val fileName = "C:/target_ready/Phase 2/CLICKSTREAM-DATA-PIPELINE/data/Input/item/item_data.csv"
//
//    // Enter your Kafka input topic name
//    val topicName = "test1"
//
//    for (line <- Source.fromFile(fileName).getLines().drop(1)) { // Dropping the column names
//      // Extract Key
//      val key = line.split(",") {
//        0
//      }
//
//      // Prepare the record to send
//      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, line)
//      println(key)
//      // Send to topic
//      producer.send(record)
//    }
//
//    producer.flush()
//    producer.close()
//  }
//}


























////=================================================================================
//
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.io.Source
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object Producer {

  def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .master("local[3]")
          .appName("Spark1")
          .getOrCreate()

        val schema = StructType(
          List(
            StructField("item_id", StringType, true),
            StructField("item_price", StringType, true),
            StructField("product_type", StringType, true),
            StructField("department_name", StringType, true)
          )
        )

        val df = spark.readStream.schema(schema).csv("C:/target_ready/Phase 2/CLICKSTREAM-DATA-PIPELINE/data/Input/item")

    df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate",false)
      .option("newRows",30)
      .start()
      .awaitTermination()

  }
}