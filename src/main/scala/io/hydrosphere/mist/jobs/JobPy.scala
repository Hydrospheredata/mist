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
  var m_error = scala.collection.mutable.Map[String, String]()
  def set(k: String, in: String): Unit = { m_error put(k, in) }
  def get(k: String): String = m_error(k)
  def remove(k: String): Unit = {m_error - k}
}

// wrapper for data in/of python
class DataWrapper{
  var m_data = scala.collection.mutable.Map[String, Any]()
  def set(k: String, in: Any): Unit = {
    m_data put(k, in)
    println("Data set", in, k)
  }
  def get(k: String): Any = m_data(k)
  def remove(k: String): Unit = {m_data - k}
}

// wrapper for SparkContext, SQLContext, HiveContext in python
class SparkContextWrapper{
  var m_conf = scala.collection.mutable.Map[String,SparkConf]()
  def getSparkConf(k: String): SparkConf = {m_conf(k)}
  def setSparkConf(k: String, conf: SparkConf): Unit = {m_conf put (k, conf)}
  def removeSparkConf(k: String): Unit = {m_conf - k}

  var m_context = scala.collection.mutable.Map[String, JavaSparkContext]()
  def getSparkContext(k: String): JavaSparkContext = m_context(k)
  def setSparkContext(k: String, sc: SparkContext): Unit = {m_context put (k, new JavaSparkContext(sc))}
  def removeSparkContext(k: String): Unit = {m_context - k}

  var m_context_wrapper = scala.collection.mutable.Map[String, ContextWrapper]()

  def addContextWrapper(id: String, contextWrapper: ContextWrapper): Unit ={
    m_context_wrapper put (id, contextWrapper)
  }
  //2var m_sqlcontext = scala.collection.mutable.Map[String, SQLContext]()
 // var m_job_py = scala.collection.mutable.Map[String, JobPy]()

  def getSqlContext(k: String): SQLContext = {
    //m_job_py(k).initSqlContext()
    //m_sqlcontext(k)
    m_context_wrapper(k).sqlContext
  }


  def setSqlContext(k: String, sqlc: SQLContext, jobPy: JobPy): Unit = {
    //m_job_py put (k, jobPy)
    //m_sqlcontext put(k, sqlc)

  }
  def removeSqlContext(k: String): Unit = {
   // m_job_py - k
    //m_sqlcontext - k
  }

  //var m_hivecontext = scala.collection.mutable.Map[String, HiveContext]()
  def getHiveContext(k: String): HiveContext = {
    //m_job_py(k).initHiveContext()
    //m_hivecontext(k)
    m_context_wrapper(k).hiveContext
  }
  def setHiveContext(k: String, hc: HiveContext, jobPy: JobPy): Unit = {
   // m_job_py put (k, jobPy)
    //m_hivecontext put(k, hc)
  }
  def removeHiveContext(k: String): Unit = {
    //m_job_py - k
    //m_hivecontext - k
  }
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

  // lazy initialisation Contexts
  override def initSqlContext(): Unit = {
    //sparkContextWrapper.setSqlContext(id, contextWrapper.sqlContext, this)
  }
  override def initHiveContext(): Unit = {
    //sparkContextWrapper.setHiveContext(id, contextWrapper.hiveContext, this)
  }

  /** Runs a job
    *
    * @return results of user jobPy
    */
  override def run(): Either[Map[String, Any], String] = {
    _status = JobStatus.Running
    try {
      var cmd = "python " + configuration.pyPath.get

      dataWrapper.set(id, configuration.parameters)

      sparkContextWrapper.addContextWrapper(id, contextWrapper)
      sparkContextWrapper.setSparkConf(id, contextWrapper.context.getConf)
      sparkContextWrapper.setSparkContext(id, contextWrapper.context)

      val gatewayServer: GatewayServer = new GatewayServer(this)
      try {
        gatewayServer.start()
        val boundPort = gatewayServer.getListeningPort

        if (boundPort == -1) {
          throw new Exception("GatewayServer to Python exception")
        } else {
          println(s" Started PythonGatewayServer on port $boundPort")
          cmd += s" $boundPort $id"
        }

        val exitCode = cmd.!
        if (exitCode != 0) {
          val errmsg = errorWrapper.get(id)
          // We must remove Error from ErrorWrapper
          errorWrapper.remove(id)
          throw new Exception("Error in python code: " + errmsg)
        }
      }
      catch {
        case e: Throwable =>
          // We must remove Data from DataWrapper
          dataWrapper.remove(id)
          throw new Exception(e)
      }
      finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
        println(" Exiting due to broken pipe from Python driver")

        sparkContextWrapper.removeSparkConf(id)
        sparkContextWrapper.removeSparkContext(id)
        sparkContextWrapper.removeSqlContext(id)
        sparkContextWrapper.removeHiveContext(id)
      }

      val result = dataWrapper.get(id)
      // We must remove Data from DataWrapper
      dataWrapper.remove(id)

      Left(Map("result" -> result))
    } catch {
      case e: Throwable =>
        println(e)
        _status = JobStatus.Aborted
        Right(e.toString)
    }
  }
}
