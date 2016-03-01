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

object DataWrapper{
  var data = scala.collection.mutable.Map[String, Any]()
  def set(k: String, in: Any) = { data put(k, in) }
  def set(k: String, in: java.util.ArrayList[Int]) = { data put(k, asScalaBuffer(in).toList) }
  def get(k: String): Any = data(k)
  def remove(k: String) = {data - k}
  def setStatementsFinished(out: String, error: Boolean) = error match {
    case true => throw new Exception(out)
    case false => println(out)
  }
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

  var m_hivecontext = scala.collection.mutable.Map[String, HiveContext ]()
  def getHiveContext(k: String): HiveContext = {m_hivecontext(k)}
  def setHiveContext(k: String, hc: HiveContext) = {m_hivecontext put(k, hc)}
  def removeHiveContext(k: String) = {m_hivecontext - k}

  def setStatementsFinished(out: String, error: Boolean) = error match {
    case true => throw new Exception(out)
    case false => println(out)
  }
}

object SimplePython {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result of the job
    */
  var cmd = "python "

  def AddPyPath(pypath: String) = {cmd = "python " + pypath }

  def ScalaSparkContextWrapper = SparkContextWrapper

  def SimpleDataWrapper = DataWrapper

  def doStuffPy(currentSessionId: String, context: SparkContext, sqlcontext: SQLContext, hivecontext: HiveContext, parameters: Map[String, Any]): Map[String, Any] = {

    val numbers: List[Int] = parameters("digits").asInstanceOf[List[Int]]

    SimpleDataWrapper.set(currentSessionId, numbers)
    ScalaSparkContextWrapper.setSparkConf(currentSessionId, context.getConf)
    ScalaSparkContextWrapper.setSparkContext(currentSessionId, context)

    //TODO lazy start SqlContext and HiveContext in depend of use at python
    ScalaSparkContextWrapper.setSqlContext(currentSessionId, sqlcontext)
    ScalaSparkContextWrapper.setHiveContext(currentSessionId, hivecontext)

    val gatewayServer: GatewayServer = new GatewayServer(SimplePython)
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort

    if (boundPort == -1) {
      println("GatewayServer failed to bind; exiting")
      throw new Exception("GatewayServer to Python exception")
    } else {
      println(s"Started PythonGatewayServer on port $boundPort")
      cmd = cmd + " " + boundPort + " " + currentSessionId
    }

    val exitCode = cmd.!
    gatewayServer.shutdown()
    ScalaSparkContextWrapper.removeSparkConf(currentSessionId)
    ScalaSparkContextWrapper.removeSparkContext(currentSessionId)
    ScalaSparkContextWrapper.removeSqlContext(currentSessionId)
    ScalaSparkContextWrapper.removeHiveContext(currentSessionId)
    println("Exiting due to broken pipe from Python driver")

    println(exitCode)
    if( exitCode!=0 )
      throw new Exception("Python error")
    val result = SimpleDataWrapper.get(currentSessionId)
    SimpleDataWrapper.remove(currentSessionId)
    Map("result" -> result)
  }

}
