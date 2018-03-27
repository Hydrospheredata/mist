package io.hydrosphere.mist.worker.runners.python

import java.io.File
import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.runners.JobRunner
import io.hydrosphere.mist.worker.runners.python.wrappers._
import io.hydrosphere.mist.worker.{NamedContext, SparkArtifact}
import mist.api.data.JsLikeData
import py4j.GatewayServer

import scala.sys.process._
import scala.util.{Failure, Success, Try}

class PythonEntryPoint(req: RunJobRequest, context: NamedContext) {

  val errorWrapper: ErrorWrapper = new ErrorWrapper
  val dataWrapper: DataWrapper = new DataWrapper
  val sparkContextWrapper: NamedContext = context
  val configurationWrapper: ConfigurationWrapper = new ConfigurationWrapper(req.params)
  val sparkStreamingWrapper: SparkStreamingWrapper = new SparkStreamingWrapper(context.setupConfiguration(req.id))

}

class PythonRunner(artifact: SparkArtifact) extends JobRunner with Logger {

  override def runSync(
    req: RunJobRequest,
    context: NamedContext
  ): Either[Throwable, JsLikeData] = {

    def pythonDriverEntry(): String = {
      val conf = context.sparkContext.getConf
      conf.getOption("spark.pyspark.driver.python").getOrElse(pythonGlobalEntry())
    }

    def pythonGlobalEntry(): String = {
      val conf = context.sparkContext.getConf
      conf.getOption("spark.pyspark.python").getOrElse("python")
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
      val selfJarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      val env = Seq(
        "PYTHONPATH" -> (Seq(pySpark.toString, py4jPath) ++ pypath).mkString(":"),
        "PYSPARK_PYTHON" -> pythonGlobalEntry(),
        "PYSPARK_DRIVER_PYTHON" -> pythonDriverEntry()
      )
      val cmd = Seq(pythonDriverEntry(), selfJarPath.toString, py4jPort.toString)
      logger.info(s"Running python task: $cmd, env $env")
      val ps = Process(cmd, None, env: _*)
      ps.!
    }

    try {
      val entryPoint = new PythonEntryPoint(req.copy(params = req.params.copy(filePath = artifact.local.toString)), context)

      val gatewayServer: GatewayServer = new GatewayServer(entryPoint, 0)
      try {
        gatewayServer.start()
        val port = gatewayServer.getListeningPort match {
          case -1 => throw new Exception("GatewayServer to Python exception")
          case port => port
        }
        logger.info(s" Started PythonGatewayServer on port $port")
        val exitCode = runPython(port)

        if (exitCode != 0 || entryPoint.errorWrapper.get().nonEmpty) {
          val errmsg = entryPoint.errorWrapper.get()
          logger.error(errmsg)
          throw new Exception("Error in python code: " + errmsg)
        }
      } finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
      }

      Try(JsLikeData.fromScala(entryPoint.dataWrapper.get)) match {
        case Success(data) => Right(data)
        case Failure(e) => Left(e)
      }
    } catch {
      case e: Throwable => Left(e)
    }
  }
}
