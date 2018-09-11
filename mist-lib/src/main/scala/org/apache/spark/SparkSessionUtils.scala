package org.apache.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

/**
  * Building SparkSession using private method  `builder.sparkContext`
  */
object SparkSessionUtils {

  def getOrCreate(sc: SparkContext, withHiveSupport: Boolean): SparkSession = {
    val builder = SparkSession
      .builder()
      .sparkContext(sc)
      .config(sc.conf)

    if (withHiveSupport) {
      sc.conf.set(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
      builder.enableHiveSupport().getOrCreate()
    } else builder.getOrCreate()

  }
}
