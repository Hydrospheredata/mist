package io.hydrosphere.mist.jobs

import java.io.File
import java.net.{URL, URLClassLoader}

import io.hydrosphere.mist.{Constants, MistJob}
import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext

/** Class-container for user jobs
  *
  * @param jobConfiguration [[io.hydrosphere.mist.jobs.JobConfiguration]] instance
  * @param contextWrapper   contexts for concrete job running
  */
private[mist] class JobJar(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper, JobRunnerName: String) extends Job {

  override val jobRunnerName = JobRunnerName

  override val configuration = jobConfiguration

  // Class with job in user jar
  private val cls = try{
    val jarFile = new File(configuration.jarPath.get)
    val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className.get)
  } catch {
    case e: Throwable =>
      throw new Exception(e)
  }

  // Scala `object` reference of user job
  private val objectRef = cls.getField("MODULE$").get(None)

  // We must add user jar into spark context
  contextWrapper.addJar(configuration.jarPath.get)

  _status = JobStatus.Initialized
  /** Runs a job
    *
    * @return results of user job
    */
  override def run(): Either[Map[String, Any], String] = {
    _status = JobStatus.Running
    try {
      val result = objectRef match {
        case objectRef: MistJob =>
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
/*
          try {
            // if user object overrides method for SparkSession(SQLContext or HiveContext context on Spark 2.0.0), use it
            cls.getDeclaredMethod("doStuff", classOf[SparkSession], classOf[Map[String, Any]])
            // run job with SQLContext or Hive on Spark 2.0.0 and return result
            return Left(objectRef.doStuff(contextWrapper.sparkSession, configuration.parameters))
          } catch {
            case _: NoSuchMethodException => // pass
          }
*/
          Right(Constants.Errors.noDoStuffMethod)
        case _ => Right(Constants.Errors.notJobSubclass)
      }

      _status = JobStatus.Stopped

      result
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = JobStatus.Aborted
        Right(e.toString)
    }
  }
}