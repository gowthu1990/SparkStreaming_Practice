package com.clearurdoubt.streaming

import com.clearurdoubt.common.CommonSchemas.studentSchema
import com.clearurdoubt.common.{SparkUtils, Student}
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, count, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter}

import scala.concurrent.duration.DurationInt

object StreamingDFs {

  val logger: Logger = LogManager.getLogger(this.getClass.getName)

  def readSocketStreams(): Unit = {
    logger.info("Starting to read Socket Text Stream")

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
      .outputMode("complete") // append and update are not supported with aggregations
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()
      .awaitTermination()
  }

  def readJsonSocketStreams(): Unit = {
    logger.info("Starting to read Socket JSON Stream")

    val df: DataFrame = SparkUtils.getSparkSession(isLocal = true, appName = "Json Socket Streams")
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), studentSchema).as("students"))
      .selectExpr("students.*")

    df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeJsonSocketStreamsToCassandra(): Unit = {
    logger.info("Starting to read Socket JSON Stream")

    val spark = SparkUtils.getSparkSession(isLocal = true, appName = "Json Socket Streams")

    import spark.implicits._

    val df: Dataset[Student] = spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), studentSchema).as("students"))
      .selectExpr("students.*")
      .as[Student]

    logger.info("About to write data to Cassandra table")

//    // Normal Way of writing
//    df
//      .writeStream
//      .foreachBatch((batch: Dataset[Student], _: Long) => {
//        batch
//          .select(col("id"), col("name"), col("year"))
//          .write
//          .cassandraFormat("students", "university")
//          .mode(SaveMode.Append)
//          .save()
//      })
//      .start()
//      .awaitTermination()


    // Advanced way of writing
    class CassandraForeachWriter extends ForeachWriter[Student] {
      val writeLogger: Logger = LogManager.getLogger(this.getClass.getName)

      val keyspace: String = "university"
      val table: String = "students"
      val connector: CassandraConnector = CassandraConnector(spark.sparkContext.getConf)

      override def open(partitionId: Long, epochId: Long): Boolean = {
        writeLogger.info("Opening the Cassandra connection")

        true
      }

      override def process(student: Student): Unit = {
        connector.withSessionDo(session => {
          session.execute(
            s"""
               |INSERT INTO $keyspace.$table("id", "name", "year")
               |VALUES(${student.id.orNull}, '${student.name.orNull}', '${student.year.orNull}')
               |""".stripMargin
          )
        })
      }

      override def close(errorOrNull: Throwable): Unit = {
        writeLogger.info("Closing the Cassandra connection")
      }
    }

    df
      .writeStream
      .foreach(new CassandraForeachWriter())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeJsonSocketStreamsToCassandra()
  }
}
