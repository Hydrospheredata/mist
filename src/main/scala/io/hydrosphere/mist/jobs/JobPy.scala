package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.MistConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import py4j.GatewayServer
import sys.process._


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
class SparkContextWrapper{

  var m_context_wrapper: ContextWrapper = _

  lazy val javaSparkContext = new JavaSparkContext(m_context_wrapper.context)

  def setContextWrapper(contextWrapper: ContextWrapper) = {m_context_wrapper = contextWrapper}

  def getSparkContext: JavaSparkContext  = javaSparkContext
  def getSparkConf:    SparkConf         = m_context_wrapper.context.getConf

  def getSqlContext:   SQLContext        = m_context_wrapper.sqlContext
  def getHiveContext:  HiveContext       = m_context_wrapper.hiveContext

}

/** Class-container for user jobs in python
  *
  * @param jobConfiguration [[io.hydrosphere.mist.jobs.JobConfiguration]] instance
  * @param contextWrapper   contexts for concrete job running
  */
private[mist] class JobPy(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper, JobRunnerName: String) extends Job {

  val dataWrapper = new DataWrapper
  val errorWrapper = new ErrorWrapper
  val sparkContextWrapper = new SparkContextWrapper

  override val jobRunnerName = JobRunnerName

  override val configuration = jobConfiguration

  /** Runs a job
    *
    * @return results of user jobPy
    */
  override def run(): Either[Map[String, Any], String] = {
    _status = JobStatus.Running
    try {
      var cmd = "python " + configuration.pyPath.get

      dataWrapper.set(configuration.parameters)

      sparkContextWrapper.setContextWrapper(contextWrapper)

      val gatewayServer: GatewayServer = new GatewayServer(this)
      try {
        gatewayServer.start()
        val boundPort = gatewayServer.getListeningPort

        if (boundPort == -1) {
          throw new Exception("GatewayServer to Python exception")
        } else {
          println(s" Started PythonGatewayServer on port $boundPort")
          cmd += s" $boundPort"
        }

        val exitCode = cmd.!
        if (exitCode != 0) {
          val errmsg = errorWrapper.get
          throw new Exception("Error in python code: " + errmsg)
        }
      }
      catch {
        case e: Throwable =>
          throw new Exception(e)
      }
      finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
        println(" Exiting due to broken pipe from Python driver")
      }

      Left(Map("result" -> dataWrapper.get))
    } catch {
      case e: Throwable =>
        println(e)
        _status = JobStatus.Aborted
        Right(e.toString)
    }
  }
}
