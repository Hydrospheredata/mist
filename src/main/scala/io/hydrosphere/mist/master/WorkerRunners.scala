package io.hydrosphere.mist.master

import java.io.File

import io.hydrosphere.mist.{ContextsSettings, WorkersSettingsConfig}
import io.hydrosphere.mist.master.models.RunMode
import io.hydrosphere.mist.utils.Logger

import scala.language.postfixOps
import scala.sys.process._

case class WorkerSettings(
  name: String,
  context: String,
  runOptions: String,
  configFilePath: String,
  jarPath: String,
  mode: RunMode
)

trait WorkerDriver {

  def run(settings: WorkerSettings): Unit

  def onStop(name: String): Unit = ()
}

/**
  * Spawn workers on the same host
  */
class LocalWorkerDriver(sparkHome: String) extends WorkerDriver with Logger {

  def run(settings: WorkerSettings): Unit = {
    import settings._

    val cmd = Seq(
      s"${sys.env("MIST_HOME")}/bin/mist-worker",
      "--runner", "local",
      "--name", name,
      "--context", context,
      "--config", configFilePath,
      "--mode", settings.mode.name,
      "--run-options", settings.runOptions)

    val builder = Process(cmd)
    builder.run(false)
  }

}

class DockerWorkerDriver(dockerHost: String, dockerPort: Int) extends WorkerDriver {

  def run(settings: WorkerSettings): Unit = {
    import settings._

    val cmd = Seq(
      s"${sys.env("MIST_HOME")}/bin/mist-worker",
      "--runner", "docker",
      "--docker-host", dockerHost,
      "--docker-port", dockerPort.toString,
      "--name", name,
      "--context", context,
      "--config", configFilePath,
      "--mode", settings.mode.name,
      "--run-options", runOptions)
    val builder = Process(cmd)
    builder.run(false)
  }
}

class ManualWorkerDriver(runCmd: String, stopCmd: String) extends WorkerDriver {

  override def run(settings: WorkerSettings): Unit = {
    import settings._
    Process(
      Seq("bash", "-c", runCmd),
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
    if (stopCmd.nonEmpty) f(stopCmd)
  }

}

class WorkerRunner(
  driver: WorkerDriver,
  contextsSettings: ContextsSettings,
  configPath: String,
  jarPath: String
) {

  def runWorker(name: String, context: String, mode: RunMode): Unit = {
    val settings = WorkerSettings(
      name = name,
      context = context,
      runOptions = contextsSettings.configFor(context).runOptions,
      configFilePath = configPath,
      jarPath = jarPath,
      mode = mode
    )
    driver.run(settings)
  }

  def onStop(name: String): Unit = driver.onStop(name)

}

object WorkerRunner {

  def create(
    configPath: String,
    workersConfig: WorkersSettingsConfig,
    contextsSettings: ContextsSettings
  ): WorkerRunner = {
    val driver = selectDriver(workersConfig)
    val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).toString
    new WorkerRunner(driver, contextsSettings, configPath, jarPath)
  }

  def selectDriver(config: WorkersSettingsConfig): WorkerDriver = {
    val runnerType = config.runner
    runnerType match {
      case "local" =>
        sys.env.get("SPARK_HOME") match {
          case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for local runner")
          case Some(home) => new LocalWorkerDriver(home)
        }
      case "docker" => new DockerWorkerDriver(config.dockerHost, config.dockerPort)
      case "manual" => new ManualWorkerDriver(config.cmd, config.cmdStop)
      case _ =>
        throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")

    }
  }
}

