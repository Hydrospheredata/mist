package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.{Constants, MistJob}
import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

trait JobJarRun extends Job{

  val cls: Class[_] = null

  val objectRef: Object = null

  val contextWrapper: ContextWrapper = null

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