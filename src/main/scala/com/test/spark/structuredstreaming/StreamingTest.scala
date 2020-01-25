package com.test.spark.structuredstreaming

import org.apache.spark.sql.functions._
import context._
import Utility._

object StreamingTest {

  def main(args: Array[String]): Unit = {

    import spark.implicits._

    spark.sparkContext.setLogLevel("INFO")

    val df = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        "wn01.itversity.com:6667,wn02.itversity.com:6667,wn03.itversity.com:6667,wn04.itversity.com:6667")
      .option("subscribe", "test_structuredstreming")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val jsonString = """{"identifier":"Source1","Data":{"empid": 20,"ename": "wNASyvg","sal": 10925,"gender": "Female","deptNo": 92}}"""

    val schema = spark.read.json(Seq(jsonString).toDS).schema

    val flattendDF = df.select(from_json($"value", schema) as "value").select("value.*").select(flattenSchema(schema): _*)

    val query = flattendDF.writeStream
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
