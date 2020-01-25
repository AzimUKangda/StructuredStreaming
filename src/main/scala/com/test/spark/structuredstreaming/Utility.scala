package com.test.spark.structuredstreaming

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import context._
import org.apache.spark.sql.{Column, DataFrame}

object Utility {

  val schemaProperties: Map[String,String] = Map("topic1" -> """{"id":"int","name":"string","department":"string"}""")

  import spark.implicits._

  def getSchema(topic:String): StructType ={
    var dataSchema: StructType = null
    try{
      val jsonString = schemaProperties(topic)
      dataSchema = spark.read.json(Seq(jsonString).toDS).schema
    }catch{
      case ex: Exception => ex.printStackTrace()
    }
    dataSchema
  }

  def getSchemaDF(topic: String): DataFrame = {
    var schemaDF: DataFrame = null
    try{
      val jsonString = schemaProperties(topic)
      schemaDF = spark.read.json(Seq(jsonString).toDS)
    }catch{
      case ex: Exception => ex.printStackTrace()
    }
    schemaDF
  }

  def flattenSchema(schema: StructType, delimiter: String = ".", prefix: String = null): Array[Column] = {
    schema.fields.flatMap(structField => {
      val codeColName = if (prefix == null) structField.name
      else prefix + "." + structField.name
      val colName = if(prefix == null) structField.name
      else prefix + delimiter + structField.name

      structField.dataType match {
        case st: StructType => flattenSchema(schema = st, delimiter = delimiter, prefix = colName)
        case _ => Array(col(codeColName).alias(colName))
      }

    })
  }

}
