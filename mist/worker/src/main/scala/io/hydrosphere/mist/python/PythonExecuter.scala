package io.hydrosphere.mist.python

import java.io.File
import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.utils.{Collections, Logger}
import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.python.wrappers.{ConfigurationWrapper, SparkStreamingWrapper}
import mist.api.args._
import mist.api.data.JsLikeData
import org.apache.spark.SparkConf
import py4j.GatewayServer

import scala.sys.process.Process
import scala.util.{Failure, Success, Try}

class Wrapper {
  var holder: Any = _

  def set(a: Any): Unit = holder = a

  def get: Any = holder
}

trait EntryPoint {
  val data: Wrapper = new Wrapper
  val error: Wrapper = new Wrapper
}


trait PythonCmd[A] extends Logger {
  val module: String

  private def selfJarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)

  def invoke(fnConf: Option[SparkConf] = None): Either[Throwable, A] = {
    def pythonDriverEntry(): String = {
      //      val conf = context.sparkContext.getConf
      fnConf.flatMap(_.getOption("spark.pyspark.driver.python")).getOrElse(pythonGlobalEntry())
    }

    def pythonGlobalEntry(): String = {
      //      val conf = context.sparkContext.getConf
      fnConf.flatMap(_.getOption("spark.pyspark.python")).getOrElse("python")
    }

    def runPython(py4jPort: Int): Int = {
      val pypath = sys.env.get("PYTHONPATH")
      val sparkHome = sys.env("SPARK_HOME")
      val pySpark = Paths.get(sparkHome, "python")
      val py4jZip = pySpark.resolve("lib").toFile.listFiles().find(_.getName.startsWith("py4j"))
      val py4jPath = py4jZip match {
        case None => throw new RuntimeException("Coudn't find py4j.zip")
        case Some(f) => f.toPath.toString
      }

      val env = Seq(
        "PYTHONPATH" -> (Seq(pySpark.toString, py4jPath) ++ pypath).mkString(":"),
        "PYSPARK_PYTHON" -> pythonGlobalEntry(),
        "PYSPARK_DRIVER_PYTHON" -> pythonDriverEntry()
      )

      val cmd = Seq(
        pythonDriverEntry(),
        selfJarPath.toString,
        "--module", module,
        "--gateway-port", py4jPort.toString
      )
      println(cmd)
      logger.info(s"Running python task: $cmd, env $env")
      val ps = Process(cmd, None, env: _*)
      ps.!
    }

    try {
      val entryPoint = mkEntryPoint()
      val gatewayServer: GatewayServer = new GatewayServer(entryPoint, 0)
      try {
        gatewayServer.start()
        val port = gatewayServer.getListeningPort match {
          case -1 => throw new Exception("GatewayServer to Python exception")
          case port => port
        }
        logger.info(s" Started PythonGatewayServer on port $port")
        val exitCode = runPython(port)
        val errorMessage = Option(entryPoint.error.get).map(_.toString).getOrElse("")
        if (exitCode != 0 || errorMessage.nonEmpty) {
          logger.error(errorMessage)
          throw new Exception("Error in python code: " + errorMessage)
        }
      } finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
      }
      dataExtractor.extract(entryPoint.data.get) match {
        case Success(yeah) => Right(yeah)
        case Failure(ex) => Left(ex)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }

  def mkEntryPoint(): EntryPoint

  def dataExtractor: DataExtractor[A]
}


trait DataExtractor[+T] {
  def extract(a: Any): Try[T]
}

class JsLikeDataExtractor extends DataExtractor[JsLikeData] {
  override def extract(a: Any): Try[JsLikeData] = a match {
    case returnValue: java.util.Map[_, _] =>
      Try(JsLikeData.fromJava(returnValue))
    case _ => Failure(new IllegalArgumentException("We should only return here ju.HashMap[String, Any] type"))
  }
}

class ArgInfoSeqDataE extends DataExtractor[Seq[ArgInfo]] {
  override def extract(a: Any): Try[Seq[ArgInfo]] = a match {
    case returnedValue: Seq[_] =>
      Try(returnedValue.map {
        case y: ArgInfo => y
        case _ => throw new IllegalArgumentException("We should only return here ArgInfo type")
      })
    case _ => Failure(new IllegalArgumentException("We should only return here Seq[_] type"))
  }
}


class ExecutePythonEntryPoint(req: RunJobRequest, context: NamedContext) extends EntryPoint {
  val sparkContextWrapper: NamedContext = context
  val configurationWrapper: ConfigurationWrapper = new ConfigurationWrapper(req.params)
  val sparkStreamingWrapper: SparkStreamingWrapper = new SparkStreamingWrapper(context.setupConfiguration(req.id))
}


class GetInfoPythonEntryPoint(
    val functionName: String,
    val filePath    : String
) extends EntryPoint


class PythonFunctionExecuter(
    req    : RunJobRequest,
    context: NamedContext
) extends PythonCmd[JsLikeData] {

  override val module: String = "python_execute_script"

  override def mkEntryPoint(): EntryPoint = new ExecutePythonEntryPoint(req, context)

  override def dataExtractor: DataExtractor[JsLikeData] = new JsLikeDataExtractor

}

class FunctionInfoPythonExecuter(jobFile: File, fnName: String) extends PythonCmd[Seq[ArgInfo]] {

  override val module: String = "metadata_extractor"

  override def mkEntryPoint(): EntryPoint = new GetInfoPythonEntryPoint(fnName, jobFile.getAbsolutePath)

  override def dataExtractor: DataExtractor[Seq[ArgInfo]] = new ArgInfoSeqDataE
}
