import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger._
object Consumer {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark1")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test1")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
      .writeStream
      .outputMode("append") 
      .format("orc")
      .option("path", "/tmp/data/output/Clickstream")
      .option("checkpointLocation", "FQDN")
      .trigger(ProcessingTime("30 seconds"))  
      .start()
      .awaitTermination()

  }
}


































//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{col, from_json}
//import org.apache.spark.sql.types.{IntegerType, StructField, StringType, StructType}
//
//object Consumer {
//  def main(args:Array[String]): Unit= {
//    val spark: SparkSession = SparkSession.builder()
//      .master("local[3]")
//      .appName("Spark1")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("ERROR")
//
//    val df = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.1.100:9092")
//      .option("subscribe", "test1")
//      .option("startingOffsets", "earliest") // From starting
//      .load()
//
////      .awaitTermination()
//
////    df.printSchema()
//    df.show()
//
////df.awaitTermination()
//    //df.show(false)
//    //org.apache.spark.sql.AnalysisException: Queries with streaming sources must be executed with writeStream.start();;
//
////    val schema = StructType(
////      List(
////        StructField("item_id", StringType, true),
////        StructField("item_price", StringType, true),
////        StructField("product_type", StringType, true),
////        StructField("department_name", StringType, true)
////      )
////    )
////
////
////
////    val person = df.selectExpr("CAST(value AS STRING)")
////      .select(from_json(col("value"), schema).as("data"))
////      .select("data.*")
////
////    /**
////     *uncomment below code if you want to write it to console for testing.
////     */
////    //    val query = person.writeStream
////    //      .format("console")
////    //      .outputMode("append")
////    //      .start()
////    //      .awaitTermination()
////
////    /**
////     *uncomment below code if you want to write it to kafka topic.
////     */
////    df.writeStream
////      .format("kafka")
////      .outputMode("append")
////      .option("kafka.bootstrap.servers", "192.168.1.100:9092")
////      .option("topic", "test1")
////      .option("checkpointLocation","FQDN")
////      .start()
////      .awaitTermination()
////    df.show()
//  }
//}
//
