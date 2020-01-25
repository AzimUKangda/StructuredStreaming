package com.test.spark.structuredstreaming

import org.apache.spark.sql.SparkSession

object context {
  lazy val spark = SparkSession.builder().appName("Streaming Application").getOrCreate()
}
