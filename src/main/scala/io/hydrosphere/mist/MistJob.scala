package io.hydrosphere.mist

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext


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

  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param context      spark sql context
    * @param parameters   user parameters
    * @return             result of the job
    */
  def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = ???

  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.hive.HiveContext]]
    * Abstract method must be overridden
    *
    * @param context      hive context
    * @param parameters   user parameters
    * @return             result of the job
    */
  def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = ???
}
