package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

// wrapper for error of python
class ErrorWrapper{
  var m_error: String = _

  def set(in: String): Unit = {
    m_error = in
  }
  def get(): String = m_error
}

// wrapper for data in/of python
class DataWrapper{
  var m_data: Any = _

  def set(in: Any): Unit = {
    m_data = in
  }
  def get: Any = m_data
}

// wrapper for SparkContext, SQLContext, HiveContext in python
class SparkContextWrapper {

  var m_context_wrapper: ContextWrapper = _

  lazy val javaSparkContext = new JavaSparkContext(m_context_wrapper.context)

  def setContextWrapper(contextWrapper: ContextWrapper) = {
    m_context_wrapper = contextWrapper
  }

  def getSparkContext: JavaSparkContext = javaSparkContext

  def getSparkConf: SparkConf = m_context_wrapper.context.getConf

  def getSqlContext: SQLContext = m_context_wrapper.sqlContext
  def getHiveContext: HiveContext = m_context_wrapper.hiveContext

}