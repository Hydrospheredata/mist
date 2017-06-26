package io.hydrosphere.mist.worker.runners.python

import java.io.File

import io.hydrosphere.mist.Messages.JobMessages.RunJobRequest
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.NamedContext
import io.hydrosphere.mist.worker.runners.JobRunner
import io.hydrosphere.mist.worker.runners.python.wrappers._
import py4j.GatewayServer

import scala.sys.process._

class PythonEntryPoint(req: RunJobRequest, context: NamedContext) {

  val errorWrapper: ErrorWrapper = new ErrorWrapper
  val dataWrapper: DataWrapper = new DataWrapper
  val sparkContextWrapper: NamedContext = context
  val configurationWrapper: ConfigurationWrapper = new ConfigurationWrapper(req.params)
  val sparkStreamingWrapper: SparkStreamingWrapper = new SparkStreamingWrapper(context.setupConfiguration(req.id))

}

class PythonRunner extends JobRunner with Logger {

  override def run(
    req: RunJobRequest,
    context: NamedContext): Either[String, Map[String, Any]] = {

    try {
      val selfJarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      var cmd = "python " + selfJarPath
      val entryPoint = new PythonEntryPoint(req, context)

      val gatewayServer: GatewayServer = new GatewayServer(entryPoint, 0)
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
        logger.info(s"Running python task: $cmd")

        val exitCode = cmd.!
        if (exitCode != 0 || entryPoint.errorWrapper.get().nonEmpty) {
          val errmsg = entryPoint.errorWrapper.get()
          logger.error(errmsg)
          throw new Exception("Error in python code: " + errmsg)
        }
      } finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
      }

      Right(entryPoint.dataWrapper.get)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        Left(e.toString)
    }
  }
}
