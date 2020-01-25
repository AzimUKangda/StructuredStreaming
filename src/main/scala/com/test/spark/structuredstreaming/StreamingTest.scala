package com.test.spark.structuredstreaming
import context._

object StreamingTest {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        "wn01.itversity.com:6667,wn02.itversity.com:6667,wn03.itversity.com:6667,wn04.itversity.com:6667")
      .option("subscribe", "test_structuredstreming")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val query = df.writeStream
      .format("parquet")
      .option("checkpointLocation", "/user/rajeshkr/checkpoint")
      .option("path", "/user/rajeshkr/data")
      .option("format", "append")
      .option("startingOffsets", "earliest")
      .outputMode("append")
      .start()

    query.awaitTermination()
  }

}
