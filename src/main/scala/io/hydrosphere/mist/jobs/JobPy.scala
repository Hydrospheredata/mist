package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.MistConfig
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import py4j.GatewayServer
import sys.process._


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

  _status = JobStatus.Initialized
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
          logger.error("GatewayServer to Python exception")
          throw new Exception("GatewayServer to Python exception")
        } else {
          logger.info(s" Started PythonGatewayServer on port $boundPort")
          cmd += s" $boundPort"
        }

        val exitCode = cmd.!
        if (exitCode != 0) {
          val errmsg = errorWrapper.get
          logger.error(errmsg)
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
        logger.info(" Exiting due to broken pipe from Python driver")
      }

      Left(Map("result" -> dataWrapper.get))
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = JobStatus.Aborted
        Right(e.toString)
    }
  }
}
