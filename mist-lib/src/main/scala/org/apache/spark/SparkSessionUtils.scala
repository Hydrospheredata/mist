package org.apache.spark

import org.apache.spark.sql.SparkSession

/**
  * Building SparkSession using private method  `builder.sparkContext`
  */
object SparkSessionUtils {

  def getOrCreate(sc: SparkContext, withHiveSupport: Boolean): SparkSession = {
    val builder = SparkSession
      .builder()
      .sparkContext(sc)
      .config(sc.conf)

    if (withHiveSupport) builder.enableHiveSupport().getOrCreate()
    else builder.getOrCreate()
  }
}
