package com.sg.services

import org.apache.spark.sql.{DataFrame, SparkSession}

object QueryService {
  def queryDF (dataFrame: DataFrame, spark: SparkSession)={
    dataFrame.printSchema()

    dataFrame.createOrReplaceTempView("people")

    val generalQueryDF = spark.sql("SELECT * FROM people")
    generalQueryDF.show()
  }
}
