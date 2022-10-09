package com.clearurdoubt.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CommonSchemas {
  val studentSchema: StructType = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("id", IntegerType, nullable = true),
    StructField("year", StringType, nullable = true)
  ))
}
