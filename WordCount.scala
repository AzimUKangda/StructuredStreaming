package com

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object WordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 10)
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val lines =
      spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    val data = lines.withColumn("words", when($"value" % 2 === 0, "Male" ).otherwise("Female"))
    val count = data.groupBy("words").count()
    //"C:\\Users\\TIS6KOR\\Documents\\Documents\\STYN\\Training\\SparkStreamingData"

    val rows = count.writeStream.outputMode(OutputMode.Update()).format("console").start()
    //val rows = count.writeStream.outputMode(OutputMode.Update()).format("csv").option("checkpointLocation", "C:\\Users\\TIS6KOR\\Documents\\Documents\\STYN\\Training\\SparkStreamingData\\Checkpoint").option("path", "C:\\Users\\TIS6KOR\\Documents\\Documents\\STYN\\Training\\SparkStreamingData\\Data").start()
    rows.awaitTermination()
  }
}

