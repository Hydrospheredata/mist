package io.hydrosphere.mist.jobs.runners.python

import java.io.File

import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobFile}
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.jobs.runners.python.wrappers.{ConfigurationWrapper, DataWrapper, ErrorWrapper}
import py4j.GatewayServer

import sys.process._

class PythonRunner(jobConfiguration: FullJobConfiguration, jobFile: JobFile, contextWrapper: ContextWrapper) extends Runner {
  override val configuration: FullJobConfiguration = jobConfiguration

  _status = Runner.Status.Initialized

  val errorWrapper = new ErrorWrapper
  val dataWrapper = new DataWrapper
  val sparkContextWrapper = contextWrapper
  val configurationWrapper = new ConfigurationWrapper(FullJobConfiguration(jobFile.file.getPath, jobConfiguration.className, jobConfiguration.namespace, jobConfiguration.parameters, jobConfiguration.external_id))

  override def run(): Either[Map[String, Any], String] = {
    _status = Runner.Status.Running
    try {
      val selfJarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      var cmd = "python " + selfJarPath

      dataWrapper.set(configuration.parameters)

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
          val errmsg = errorWrapper.get()
          logger.error(errmsg)
          throw new Exception("Error in python code: " + errmsg)
        }
      } finally {
        // We must shutdown gatewayServer
        gatewayServer.shutdown()
        logger.info(" Exiting due to broken pipe from Python driver")
      }

      Left(Map("result" -> dataWrapper.get))
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = Runner.Status.Aborted
        Right(e.toString)
    }
  }
}
