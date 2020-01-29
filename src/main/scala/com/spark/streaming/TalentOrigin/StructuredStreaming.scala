package streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.log4j._

object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Structured Streaming").master("local[*]").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val retailDataSchema = StructType(Array(StructField("SerialNumber", IntegerType, true),
      StructField("InvoiceNo", StringType, true),
      StructField("StockCode", StringType, true),
      StructField("Description", StringType, true),
      StructField("Quantity", IntegerType, true),
      StructField("InvoiceDate", TimestampType, true),
      StructField("UnitPrice", DoubleType, true),
      StructField("CustomerID", DoubleType, true),
      StructField("Country", StringType, true),
      StructField("InvoiceTimestamp", TimestampType, true)))


    val streamingData = spark
      //.read.schema(retailDataSchema).option("header", "true").csv("C:/Users/RAJESH/Desktop/Structured-Streaming/TalentOrigin/temp_working") //.drop("SerialNumber")
      .readStream
      .schema(retailDataSchema).option("header", "true")
      .csv("C:/Users/RAJESH/Desktop/Structured-Streaming/TalentOrigin/source-code-master/datasets/retail-data") //.drop("SerialNumber")

   val filteredData = streamingData//.filter("Country = 'United Kingdom'")

     val query = filteredData.writeStream
       .format("console")
       .queryName("filteredByCountry")
       .option("truncate","false")
       .outputMode(OutputMode.Update())
       .start()

     query.awaitTermination()
  }
}
