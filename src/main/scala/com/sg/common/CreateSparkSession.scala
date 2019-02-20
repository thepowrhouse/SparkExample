package com.sg.common

import org.apache.spark.sql.SparkSession

object CreateSparkSession {
  def getSparkSession(appName: String) ={
    SparkSession
      .builder
      .appName(appName)
      .config("spark.master", "local")
      //.enableHiveSupport()
      .getOrCreate()
  }
}
