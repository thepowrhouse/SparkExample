package com.sg

import com.sg.common.CreateSparkSession
import com.sg.processors.FileProcessor
import com.sg.services.QueryService

object App {
  def main(args: Array[String]): Unit = {
    val spark = CreateSparkSession.getSparkSession("Sample App")

    val peopleDF = FileProcessor.loadJson(spark)
    QueryService.queryDF(peopleDF, spark)

    val textDF = FileProcessor.loadText(spark)
    QueryService.queryDF(textDF, spark)

    val csvDF = FileProcessor.loadCsv(spark)
    QueryService.queryDF(csvDF, spark)

    val textSchemaDF = FileProcessor.loadTextInferSchema(spark)
    QueryService.queryDF(textSchemaDF, spark)

    spark.stop()
  }
}
