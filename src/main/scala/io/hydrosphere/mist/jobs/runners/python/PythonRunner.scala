package io.hydrosphere.mist.jobs.runners.python

import java.io.File

import io.hydrosphere.mist.contexts.NamedContext
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.jobs.runners.python.wrappers._
import io.hydrosphere.mist.jobs.{JobDetails, JobFile}
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError
import py4j.GatewayServer

import scala.sys.process._

class PythonRunner(
  override val job: JobDetails,
  jobFile: JobFile,
  context: NamedContext) extends Runner {

  //TODO: stop is kill??
  override def stop(): Unit = {

  }

  val errorWrapper: ErrorWrapper = new ErrorWrapper
  val dataWrapper: DataWrapper = new DataWrapper
  val sparkContextWrapper: NamedContext = context
  val globalPublisherWrapper: GlobalPublisherWrapper = new GlobalPublisherWrapper()
  val configurationWrapper: ConfigurationWrapper = new ConfigurationWrapper(job.configuration)
  val sparkStreamingWrapper: SparkStreamingWrapper = new SparkStreamingWrapper(context.setupConfiguration)

  override def run(): JobResponseOrError = {
    try {

      val selfJarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      var cmd = "python " + selfJarPath

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
        logger.info(s"Running python task: $cmd")

        val exitCode = cmd.!
        if (exitCode != 0 || errorWrapper.get().nonEmpty) {
          val errmsg = errorWrapper.get()
          logger.error(errmsg)
          throw new Exception("Error in python code: " + errmsg)
        }
      } finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
        logger.info(" Exiting due to broken pipe from Python driver")
      }

      Left(dataWrapper.get)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        Right(e.toString)
    }
  }

}
