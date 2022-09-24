package com.clearurdoubt.streaming

import com.clearurdoubt.common.SparkUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length}
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
      .withColumn("input", col("value"))
      .filter(length(col("input")) > 5)

    transformedDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readSocketStreams()
  }
}
