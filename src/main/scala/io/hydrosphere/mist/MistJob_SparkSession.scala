package io.hydrosphere.mist

import org.apache.spark.SparkContext
import org.apache.spark.sql._
//import org.apache.spark.sql.hive.HiveContext


/** Provides abstract class for user spark jobs */
class MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context      spark context
    * @param parameters   user parameters
    * @return             result of the job
    */
  def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = ???

  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SparkSession]]
    * Abstract method must be overridden
    *
    * @param context    spark sessioin
    * @param parameters user parameters
    * @return result of the job
    */
  def doStuff(context: SparkSession, parameters: Map[String, Any]): Map[String, Any] = ???
}
