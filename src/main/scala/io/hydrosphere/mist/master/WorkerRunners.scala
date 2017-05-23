package io.hydrosphere.mist.master

import java.io.File

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.utils.Logger

import scala.sys.process._
import scala.language.postfixOps

case class WorkerSettings(
  name: String,
  runOptions: String,
  configFilePath: String,
  jarPath: String
)

trait WorkerRunner {

  def run(settings: WorkerSettings): Unit

  def onStop(name: String): Unit = ()
}

/**
  * Spawn workers on the same host
  */
class LocalWorkerRunner(sparkHome: String) extends WorkerRunner with Logger {


  def run(settings: WorkerSettings): Unit = {
    import settings._

    val cmd = Seq(
      s"${sys.env("MIST_HOME")}/bin/mist-worker",
      "--runner", "local",
      "--namespace", name,
      "--config", configFilePath,
      "--run-options", settings.runOptions)

    val builder = Process(cmd)
    builder.run(false)
  }

}

object DockerWorkerRunner extends WorkerRunner {

  def run(settings: WorkerSettings): Unit = {
    import settings._

    val cmd = Seq(
      s"${sys.env("MIST_HOME")}/bin/mist-worker",
      "--runner", "docker",
      "--docker-host", MistConfig.Workers.dockerHost,
      "--docker-port", MistConfig.Workers.dockerPort.toString,
      "--namespace", name,
      "--config", configFilePath,
      "--run-options", runOptions)
    val builder = Process(cmd)
    builder.run(false)
  }
}

object ManualWorkerRunner extends WorkerRunner {

  override def run(settings: WorkerSettings): Unit = {
    import settings._
    Process(
      Seq("bash", "-c", MistConfig.Workers.cmd),
      None,
      "MIST_WORKER_NAMESPACE" -> name,
      "MIST_WORKER_CONFIG" -> configFilePath,
      "MIST_WORKER_JAR_PATH" -> jarPath,
      "MIST_WORKER_RUN_OPTIONS" -> runOptions
    ).run(false)
  }

  override def onStop(name: String): Unit = withStopCommand { cmd =>
    Process(
      Seq("bash", "-c", cmd),
      None,
      "MIST_WORKER_NAMESPACE" -> name
    ).run(false)
  }

  private def withStopCommand(f: String => Unit): Unit = {
    val cmd = MistConfig.Workers.cmdStop
    if (cmd.nonEmpty) f(cmd)
  }

}

