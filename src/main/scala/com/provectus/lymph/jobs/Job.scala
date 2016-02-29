package com.provectus.lymph.jobs

import java.io.File
import java.net.{URL, URLClassLoader}

import com.provectus.lymph.pythonexecuter.SimplePython
import com.provectus.lymph.{Constants, LymphJob}
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
    val jarFile = new File(configuration.jarPath.get)
    val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className.get)
  }

  // Scala `object` reference of user job
  println(cls.getDeclaredFields.toList.toString())
  private val objectRef = cls.getDeclaredField("MODULE$").get(null)

  // We must add user jar into spark context
  contextWrapper.addJar(configuration.jarPath.get)

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
          return Right(Constants.Errors.noDoStuffMethod)
        case _ => return Right(Constants.Errors.notJobSubclass)
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

private[lymph] class JobPy(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper){

  final val id = java.util.UUID.randomUUID.toString

  private val configuration = jobConfiguration
  private var _status = JobStatus.Initialized

  /** Status getter
    *
    * @return [[JobStatus]]
    */
  def status = _status

  SimplePython.AddPyPath(configuration.pyPath.getOrElse(""))

  /** Runs a job
    *
    * @return results of user jobPy
    */
  def run(): Either[Map[String, Any], String] = {
    _status = JobStatus.Running
    try {
      val result = Left(SimplePython.doStuffPy(id, contextWrapper.context, contextWrapper.sqlContext, contextWrapper.hiveContext, configuration.parameters))
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