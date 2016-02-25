package com.provectus.lymph.jobs

import java.io.File
import java.net.{URL, URLClassLoader}

import org.apache.spark.api.java.JavaSparkContext
import py4j.GatewayServer

import com.provectus.lymph.{Constants, LymphJob}
import com.provectus.lymph.contexts.ContextWrapper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import sys.process._

/** Job state statuses */
private[lymph] object JobStatus extends Enumeration {
  type JobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

trait Job{

  final val id = java.util.UUID.randomUUID.toString

  protected var _status = JobStatus.Initialized

  def initSqlContext: Unit = ???
  def initHiveContext: Unit = ???

  /** Status getter
    *
    * @return [[JobStatus]]
    */
  def status = _status

  def run(): Either[Map[String, Any], String]
}

/** Class-container for user jobs
  *
  * @param jobConfiguration [[com.provectus.lymph.jobs.JobConfiguration]] instance
  * @param contextWrapper   contexts for concrete job running
  */
private[lymph] class JobJar(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper) extends Job {

  private val configuration = jobConfiguration

  // Class with job in user jar
  private val cls = {
    val jarFile = new File(configuration.jarPath.get)
    val classLoader = new URLClassLoader(Array[URL](jarFile.toURI.toURL), getClass.getClassLoader)
    classLoader.loadClass(configuration.className.get)
  }

  // Scala `object` reference of user job
  private val objectRef = cls.getField("MODULE$").get(null)

  // We must add user jar into spark context
  contextWrapper.addJar(configuration.jarPath.get)

  /** Runs a job
    *
    * @return results of user job
    */
  override def run(): Either[Map[String, Any], String] = {
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
  * @param jobConfiguration [[com.provectus.lymph.jobs.JobConfiguration]] instance
  * @param contextWrapper   contexts for concrete job running
  */
private[lymph] class JobPy(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper) extends Job {

  private val configuration = jobConfiguration

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

object Job{
  def apply(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper):Job = {
    val path = jobConfiguration.jarPath.getOrElse(jobConfiguration.pyPath.getOrElse(""))
    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => return new JobJar(jobConfiguration, contextWrapper)
      case "py" => return new JobPy(jobConfiguration, contextWrapper)
      case _ => throw new Exception("Error, you must specify the path to .jar or .py file")
    }
  }
}