package com.sg.processors

import com.sg.models.Person
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object FileProcessor {
  def loadJson(sparkSession: SparkSession): DataFrame ={
    val jsonFile = "src/main/resources/people.json" // Should be some file on your system
    val peopleDF = sparkSession.read.json(jsonFile)
    return peopleDF
  }

  def loadText(sparkSession: SparkSession): DataFrame ={
    val textFile = "src/main/resources/people.txt"
    val peoplerowRDD = sparkSession.sparkContext.textFile(textFile)
                               .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))


    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val peopleDF = sparkSession.createDataFrame(peoplerowRDD, schema)
    return peopleDF

  }

  def loadTextInferSchema(sparkSession: SparkSession): DataFrame={
    import sparkSession.implicits._

    val textFile = "src/main/resources/people.txt"
    val peopleDF = sparkSession.sparkContext
      .textFile(textFile)
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    return peopleDF
  }

  def loadCsv(sparkSession: SparkSession): DataFrame ={
    val csvFile = "src/main/resources/people.csv"
    val peopleDF = sparkSession.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(csvFile)
    return peopleDF
  }
}
