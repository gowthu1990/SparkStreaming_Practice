package com.clearurdoubt.common

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession(isLocal: Boolean, appName: String): SparkSession = {
    val sparkBuilder = SparkSession.builder
      .appName(appName)
      .enableHiveSupport()

    if(isLocal) sparkBuilder.master("local[*]").getOrCreate()
    else sparkBuilder.getOrCreate()
  }
}

