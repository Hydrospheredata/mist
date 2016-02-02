package com.provectus.lymph.jobs

import java.io.File
import java.net.{URL, URLClassLoader}

import com.provectus.lymph.LymphJob
import com.provectus.lymph.contexts.ContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

private[lymph] object JobStatus extends Enumeration {
  type SparkJobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

private[lymph] class Job(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper) {

  final val id = java.util.UUID.randomUUID.toString

  private val configuration = jobConfiguration
  private var _status = JobStatus.Initialized

  def status = _status

    val jarFile = new File(configuration.jarPath)
    val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
    val cls = classLoader.loadClass(configuration.className)


    val objectRef = cls.getField("MODULE$").get(null)

  contextWrapper.addJar(configuration.jarPath)

  def run(): Map[String, Any] = {
    _status = JobStatus.Running
    try {
      val result = objectRef match {
        case objectRef: LymphJob =>
          try {
            cls.getDeclaredMethod("doStuff", classOf[SparkContext], classOf[Map[String, Any]])
            return objectRef.doStuff(contextWrapper.context, configuration.parameters)
          } catch {
            case _: NoSuchMethodException => println("No method for SparkContext") // pass
          }
          try {
            cls.getDeclaredMethod("doStuff", classOf[SQLContext], classOf[Map[String, Any]])
            return objectRef.doStuff(contextWrapper.sqlContext, configuration.parameters)
          } catch {
            case _: NoSuchMethodException => println("No method for SQLContext") // pass
          }
          try {
            cls.getDeclaredMethod("doStuff", classOf[HiveContext], classOf[Map[String, Any]])
            return objectRef.doStuff(contextWrapper.hiveContext, configuration.parameters)
          } catch {
            case _: NoSuchMethodException => println("No method for HiveContext") // pass
          }
          null
        // TODO: Own exceptions
        case _ => throw new Exception("External module is not LymphJob subclass")
      }

      _status = JobStatus.Stopped

      result
    } catch {
      case e: Throwable =>
        println(e)
        _status = JobStatus.Aborted
        // TODO: return, not throw
        throw e
    }
  }

}
