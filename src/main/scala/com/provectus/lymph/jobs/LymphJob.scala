package com.provectus.lymph.jobs

import java.io.File
import java.net.{URL, URLClassLoader}

import com.provectus.lymph.LymphJob
import com.provectus.lymph.contexts.ContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

private[lymph] object LymphJobStatus extends Enumeration {
  type SparkJobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

private[lymph] class LymphJob(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper) {
  final val id = java.util.UUID.randomUUID.toString

  private val configuration = jobConfiguration
  private var _status = LymphJobStatus.Initialized

  def status = _status

  val objectRef = {
      val jarFile = new File(configuration.jarPath)
      val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
      val cls = classLoader.loadClass(configuration.className)

      cls.getField("MODULE$").get(null).asInstanceOf[LymphJob]
  }

  contextWrapper.addJar(configuration.jarPath)

  def run(): Map[String, Any] = {
    _status = LymphJobStatus.Running
    try {
      val result = objectRef match {
        case objectRef: LymphJob =>
          try {
            objectRef.getClass.getMethod("doStuff", classOf[SparkContext], classOf[Map[String, Any]])
            return objectRef.doStuff(contextWrapper.context, configuration.parameters)
          } catch {
            case _: NoSuchMethodException => // pass
          }
          try {
            objectRef.getClass.getMethod("doStuff", classOf[SQLContext], classOf[Map[String, Any]])
            return objectRef.doStuff(contextWrapper.sqlContext, configuration.parameters)
          } catch {
            case _: NoSuchMethodException => // pass
          }
          try {
            objectRef.getClass.getMethod("doStuff", classOf[HiveContext], classOf[Map[String, Any]])
            return objectRef.doStuff(contextWrapper.hiveContext, configuration.parameters)
          } catch {
            case _: NoSuchMethodException => // pass
          }
          null
        // TODO: Own exceptions
        case _ => throw new Exception("External module is not LymphJob subclass")
      }

      _status = LymphJobStatus.Stopped

      result
    } catch {
      case e: Exception =>
        println(e)
        _status = LymphJobStatus.Aborted
        throw e
    }
  }

}
