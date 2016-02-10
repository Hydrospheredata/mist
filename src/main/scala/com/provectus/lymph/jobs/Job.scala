package com.provectus.lymph.jobs

import java.io.File
import java.net.{URL, URLClassLoader}

import com.provectus.lymph.LymphJob
import com.provectus.lymph.contexts.ContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/** Job state statuses */
private[lymph] object JobStatus extends Enumeration {
  type JobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

/** Class-container for user jobs
  *
  * @param jobConfiguration   [[com.provectus.lymph.jobs.JobConfiguration]] instance
  * @param contextWrapper     contexts for concrete job running
  */
private[lymph] class Job(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper) {

  final val id = java.util.UUID.randomUUID.toString

  private val configuration = jobConfiguration
  private var _status = JobStatus.Initialized

  /** Status getter
    *
    * @return [[JobStatus]]
    */
  def status = _status

  // Class with job in user jar
  private val cls = {
    val jarFile = new File(configuration.jarPath)
    val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className)
  }

  // Scala `object` reference of user job
  private val objectRef = cls.getField("MODULE$").get(null)

  // We must add user jar into spark context
  contextWrapper.addJar(configuration.jarPath)

  /** Runs a job
    *
    * @return results of user job
    */
  def run(): Either[Map[String, Any], String] = {
    _status = JobStatus.Running
    try {
      val result = objectRef match {
        case objectRef: LymphJob =>
          try {
            // if user object overrides method for SparkContext, use it
            cls.getDeclaredMethod("doStuff", classOf[SparkContext], classOf[Map[String, Any]])
            // run job with SparkContext and return result
            return Left(objectRef.doStuff(contextWrapper.context, configuration.parameters))
          } catch {
            case _: NoSuchMethodException => // pass
          }
          try {
            // if user object overrides method for SQLContext, use it
            cls.getDeclaredMethod("doStuff", classOf[SQLContext], classOf[Map[String, Any]])
            // run job with SQLContext and return result
            return Left(objectRef.doStuff(contextWrapper.sqlContext, configuration.parameters))
          } catch {
            case _: NoSuchMethodException => // pass
          }
          try {
            // if user object overrides method for HiveContext, use it
            cls.getDeclaredMethod("doStuff", classOf[HiveContext], classOf[Map[String, Any]])
            // run job with HiveContext and return result
            return Left(objectRef.doStuff(contextWrapper.hiveContext, configuration.parameters))
          } catch {
            case _: NoSuchMethodException => // pass
          }
          return Right(s"No method in class ${configuration.className}")
        // TODO: Own exceptions
        case _ => throw new Exception("External module is not LymphJob subclass")
      }

      _status = JobStatus.Stopped

      result
    } catch {
      case e: Throwable =>
        println(e)
        _status = JobStatus.Aborted
        Right(e.toString)
    }
  }

}
