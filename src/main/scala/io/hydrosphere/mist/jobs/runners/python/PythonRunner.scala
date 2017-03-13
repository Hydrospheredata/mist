package io.hydrosphere.mist.jobs.runners.python

import java.io.File

import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.jobs.runners.python.wrappers._
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobFile, MistJobConfiguration}
import io.hydrosphere.mist.lib.ContextWrapper
import py4j.GatewayServer

import scala.sys.process._

class PythonRunner(jobConfiguration: FullJobConfiguration, jobFile: JobFile, contextWrapper: ContextWrapper) extends Runner {
  override val configuration: FullJobConfiguration = jobConfiguration

  _status = Runner.Status.Initialized

  override def stopStreaming(): Unit = sparkStreamingWrapper.stopStreaming()

  val errorWrapper: ErrorWrapper = new ErrorWrapper
  val dataWrapper: DataWrapper = new DataWrapper
  val sparkContextWrapper: ContextWrapper = contextWrapper
  val configurationWrapper: ConfigurationWrapper = new ConfigurationWrapper(
    MistJobConfiguration(
      jobFile.file.getPath,
      jobConfiguration.className,
      jobConfiguration.namespace,
      jobConfiguration.parameters,
      jobConfiguration.externalId ))
  val mqttPublisher: MqttPublisherWrapper = new MqttPublisherWrapper
  val sparkStreamingWrapper: SparkStreamingWrapper = new SparkStreamingWrapper(sparkContextWrapper)

  override def run(): Either[Map[String, Any], String] = {
    _status = Runner.Status.Running
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

      _status = Runner.Status.Stopped

      Left(dataWrapper.get)
    } catch {
      case e: Throwable =>
        logger.error(e.getMessage, e)
        _status = Runner.Status.Aborted
        Right(e.toString)
    }
  }

}
