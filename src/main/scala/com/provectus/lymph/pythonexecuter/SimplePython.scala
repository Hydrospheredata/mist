package com.provectus.lymph.pythonexecuter

/**
  * Created by lblokhin on 24.02.16.
  */
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext

import py4j.GatewayServer
import sys.process._

import scala.collection.JavaConversions._

object ErrorWrapper{
  var m_error = scala.collection.mutable.Map[String, String]()
  def set(k: String, in: String) = { m_error put(k, in) }
  def get(k: String): String = m_error(k)
  def remove(k: String) = {m_error - k}
}

object DataWrapper{
  var m_data = scala.collection.mutable.Map[String, Any]()
  def set(k: String, in: Any) = { m_data put(k, in) }
  def set(k: String, in: java.util.ArrayList[Int]) = { m_data put(k, asScalaBuffer(in).toList) }
  def get(k: String): Any = m_data(k)
  def remove(k: String) = {m_data - k}
}

object SparkContextWrapper{

  var m_conf = scala.collection.mutable.Map[String,SparkConf]()
  def getSparkConf(k: String): SparkConf = {m_conf(k)}
  def setSparkConf(k: String, conf: SparkConf) = {m_conf put (k, conf)}
  def removeSparkConf(k: String) = {m_conf - k}

  var m_context = scala.collection.mutable.Map[String, JavaSparkContext]()
  def getSparkContext(k: String): JavaSparkContext = m_context(k)
  def setSparkContext(k: String, sc: SparkContext) = {m_context put (k, new JavaSparkContext(sc))}
  def removeSparkContext(k: String) = {m_context - k}

  var m_sqlcontext = scala.collection.mutable.Map[String, SQLContext]()
  def getSqlContext(k: String): SQLContext = {m_sqlcontext(k)}
  def setSqlContext(k: String, sqlc: SQLContext) = {m_sqlcontext put(k, sqlc)}
  def removeSqlContext(k: String) = {m_sqlcontext -k}

  var m_hivecontext = scala.collection.mutable.Map[String, HiveContext]()
  def getHiveContext(k: String): HiveContext = {m_hivecontext(k)}
  def setHiveContext(k: String, hc: HiveContext) = {m_hivecontext put(k, hc)}
  def removeHiveContext(k: String) = {m_hivecontext - k}
}

object SimplePython {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result of the job
    */
  var cmd = scala.collection.mutable.Map[String, String]()

  def addPyPath(jobId: String, pyPath: String) = {cmd put(jobId, "python " + pyPath)}

  def sparkContextWrapper = SparkContextWrapper

  def dataWrapper = DataWrapper

  def errorWrapper = ErrorWrapper

  def doStuffPy(jobId: String, context: SparkContext, sqlcontext: SQLContext, hivecontext: HiveContext, parameters: Map[String, Any]): Map[String, Any] = {

    val numbers: List[Int] = parameters("digits").asInstanceOf[List[Int]]
    dataWrapper.set(jobId, numbers)

    sparkContextWrapper.setSparkConf(jobId, context.getConf)
    sparkContextWrapper.setSparkContext(jobId, context)

    //TODO lazy start SqlContext and HiveContext in depend of use at python
    sparkContextWrapper.setSqlContext(jobId, sqlcontext)
    sparkContextWrapper.setHiveContext(jobId, hivecontext)

    val gatewayServer: GatewayServer = new GatewayServer(SimplePython)
    try {
      gatewayServer.start()
      val boundPort: Int = gatewayServer.getListeningPort

      if (boundPort == -1) {
        throw new Exception("GatewayServer to Python exception")
      } else {
        println(s" Started PythonGatewayServer on port $boundPort")
        cmd put (jobId, cmd(jobId) + " " + boundPort + " " + jobId)
      }

      val exitCode = cmd(jobId).!
      if( exitCode!=0 ) {
        lazy val errmsg = errorWrapper.get(jobId)
        errorWrapper.remove(jobId)
        throw new Exception("Error in python code: " + errmsg)
      }
    }
    catch {
      case e: Throwable => {
        dataWrapper.remove(jobId)
        throw new Exception(e)
      }
    }
    finally {
      gatewayServer.shutdown()
      println("Exiting due to broken pipe from Python driver")
      cmd - jobId
      sparkContextWrapper.removeSparkConf(jobId)
      sparkContextWrapper.removeSparkContext(jobId)
      sparkContextWrapper.removeSqlContext(jobId)
      sparkContextWrapper.removeHiveContext(jobId)
    }
    val result = dataWrapper.get(jobId)
    dataWrapper.remove(jobId)

    Map("result" -> result)
  }
}
