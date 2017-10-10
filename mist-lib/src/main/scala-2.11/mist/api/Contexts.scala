package mist.api

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkSessionUtils


trait Contexts extends BaseContexts {

  val sparkSession: ArgDef[SparkSession] = sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, false))
  val sparkSessionWithHive: ArgDef[SparkSession] = sparkContext.map(sc => SparkSessionUtils.getOrCreate(sc, true))
}

object Contexts extends Contexts
