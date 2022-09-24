package com.clearurdoubt.streaming

import com.clearurdoubt.common.SparkUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration.DurationInt

object StreamingDFs {
  def readSocketStreams(): Unit = {
    val df: DataFrame = SparkUtils.getSparkSession(isLocal = true, appName = "Socket Streams")
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    val transformedDF: DataFrame = df
      .withColumnRenamed("value","input")
      .groupBy("input").agg(count("input").as("count"))

    transformedDF.writeStream
      .format("console")
      .outputMode("complete") // append and update are not not supported with aggregations
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readSocketStreams()
  }
}
