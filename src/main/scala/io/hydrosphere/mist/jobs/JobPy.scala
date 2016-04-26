package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.api.java.JavaSparkContext
import py4j.GatewayServer
import sys.process._


//wrapper for error of python
object ErrorWrapper{
  var m_error = scala.collection.mutable.Map[String, String]()
  def set(k: String, in: String) = { m_error put(k, in) }
  def get(k: String): String = m_error(k)
  def remove(k: String) = {m_error - k}
}

//wrapper for data in/of python
object DataWrapper{
  var m_data = scala.collection.mutable.Map[String, Any]()
  def set(k: String, in: Any) = { m_data put(k, in) }
  def get(k: String): Any = m_data(k)
  def remove(k: String) = {m_data - k}
}

//wrapper for SparkContext, SQLContext, HiveContext in python
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
  def getSqlContext(k: String): SQLContext = {
    InMemoryJobRepository.get(new JobByIdSpecification(k)).get.initSqlContext
    m_sqlcontext(k)
  }
  def setSqlContext(k: String, sqlc: SQLContext) = {m_sqlcontext put(k, sqlc)}
  def removeSqlContext(k: String) = {m_sqlcontext -k}

  var m_hivecontext = scala.collection.mutable.Map[String, HiveContext]()
  def getHiveContext(k: String): HiveContext = {
    InMemoryJobRepository.get(new JobByIdSpecification(k)).get.initHiveContext
    m_hivecontext(k)
  }
  def setHiveContext(k: String, hc: HiveContext) = {m_hivecontext put(k, hc)}
  def removeHiveContext(k: String) = {m_hivecontext - k}
}

/** Class-container for user jobs in python
  *
  * @param jobConfiguration [[io.hydrosphere.mist.jobs.JobConfiguration]] instance
  * @param contextWrapper   contexts for concrete job running
  */
private[mist] class JobPy(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper) extends Job {

  override val configuration = jobConfiguration

  def sparkContextWrapper = SparkContextWrapper

  def dataWrapper = DataWrapper

  def errorWrapper = ErrorWrapper

  //lazy initialisation Contexts
  override def initSqlContext = {sparkContextWrapper.setSqlContext(id, contextWrapper.sqlContext)}
  override def initHiveContext = {sparkContextWrapper.setHiveContext(id, contextWrapper.hiveContext)}

  /** Runs a job
    *
    * @return results of user jobPy
    */
  override def run(): Either[Map[String, Any], String] = {

    _status = JobStatus.Running

    try {
      var cmd = "python " + configuration.pyPath.get

      dataWrapper.set(id, configuration.parameters)

      sparkContextWrapper.setSparkConf(id, contextWrapper.context.getConf)
      sparkContextWrapper.setSparkContext(id, contextWrapper.context)

      val gatewayServer: GatewayServer = new GatewayServer(this)
      try {
        gatewayServer.start()
        val boundPort: Int = gatewayServer.getListeningPort

        if (boundPort == -1) {
          throw new Exception("GatewayServer to Python exception")
        } else {
          println(s" Started PythonGatewayServer on port $boundPort")
          cmd = cmd + " " + boundPort + " " + id
        }

        val exitCode = cmd.!
        if( exitCode!=0 ) {
          lazy val errmsg = errorWrapper.get(id)
          //We must remove Error from ErrorWrapper
          errorWrapper.remove(id)
          throw new Exception("Error in python code: " + errmsg)
        }
      }
      catch {
        case e: Throwable => {
          //We must remove Data from DataWrapper
          dataWrapper.remove(id)
          throw new Exception(e)
        }
      }
      finally {
        //We must shutdown gatewayServer
        gatewayServer.shutdown()
        println(" Exiting due to broken pipe from Python driver")

        sparkContextWrapper.removeSparkConf(id)
        sparkContextWrapper.removeSparkContext(id)
        sparkContextWrapper.removeSqlContext(id)
        sparkContextWrapper.removeHiveContext(id)
      }

      val result = dataWrapper.get(id)
      //We must remove Data from DataWrapper
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