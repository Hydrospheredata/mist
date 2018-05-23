package io.hydrosphere.mist.python

import java.io.File
import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.{EnvInfo, RunJobRequest}
import io.hydrosphere.mist.core.PythonEntrySettings
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.MistScContext
import io.hydrosphere.mist.worker.runners.python.wrappers.{ConfigurationWrapper, SparkStreamingWrapper}
import mist.api.ArgInfo
import mist.api.data.JsData
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

  def pythonEntrySettings: PythonEntrySettings

  private def selfJarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)

  def invoke(fnConf: Option[SparkConf] = None): Either[Throwable, A] = {
    def runPython(py4jPort: Int): Int = {
      val pypath = sys.env.get("PYTHONPATH")
      val sparkHome = sys.env("SPARK_HOME")
      val pySpark = Paths.get(sparkHome, "python")
      val py4jZip = pySpark.resolve("lib").toFile.listFiles().find(_.getName.startsWith("py4j"))
      val py4jPath = py4jZip match {
        case None => throw new RuntimeException("Couldn't find py4j.zip")
        case Some(f) => f.toPath.toString
      }

      val settings = pythonEntrySettings

      val env = Seq(
        "PYTHONPATH" -> (Seq(pySpark.toString, py4jPath) ++ pypath).mkString(":"),
        "PYSPARK_PYTHON" -> settings.global,
        "PYSPARK_DRIVER_PYTHON" -> settings.driver
      )

      val cmd = Seq(
        settings.driver,
        selfJarPath.toString,
        "--module", module,
        "--gateway-port", py4jPort.toString
      )
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

class JsLikeDataExtractor extends DataExtractor[JsData] {
  override def extract(a: Any): Try[JsData] = a match {
    case returnValue: java.util.Map[_, _] =>
      Try(JsData.fromJava(returnValue))
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


class ExecutePythonEntryPoint(req: RunJobRequest, context: MistScContext) extends EntryPoint {
  val sparkContextWrapper: MistScContext = context
  val configurationWrapper: ConfigurationWrapper = new ConfigurationWrapper(req.params)
  val sparkStreamingWrapper: SparkStreamingWrapper = new SparkStreamingWrapper(context.streamingDuration)
}


class GetInfoPythonEntryPoint(
    val functionName: String,
    val filePath    : String
) extends EntryPoint


class PythonFunctionExecutor(
    req    : RunJobRequest,
    context: MistScContext
) extends PythonCmd[JsData] {

  override val module: String = "python_execute_script"

  override def mkEntryPoint(): EntryPoint = new ExecutePythonEntryPoint(req, context)

  override def dataExtractor: DataExtractor[JsData] = new JsLikeDataExtractor

  override def pythonEntrySettings: PythonEntrySettings = {
    import PythonEntrySettings._

    val conf = context.sc.getConf
    val driver = conf.getOption(PythonDriverKey).orElse(conf.getOption(PythonGlobalKey)).getOrElse(PythonDefault)
    val global = conf.getOption(PythonGlobalKey).getOrElse(PythonDefault)
    PythonEntrySettings(driver, global)
  }
}

class FunctionInfoPythonExecutor(
  jobFile: File,
  fnName: String,
  info: EnvInfo
) extends PythonCmd[Seq[ArgInfo]] {

  override val module: String = "metadata_extractor"

  override def mkEntryPoint(): EntryPoint = new GetInfoPythonEntryPoint(fnName, jobFile.getAbsolutePath)

  override def dataExtractor: DataExtractor[Seq[ArgInfo]] = new ArgInfoSeqDataE

  override def pythonEntrySettings: PythonEntrySettings = info.pythonSettings
}
