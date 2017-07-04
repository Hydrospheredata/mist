package io.hydrosphere.mist.master

import java.io.File

import io.hydrosphere.mist.{ContextsSettings, EndpointConfig, LogServiceConfig, MasterConfig, WorkersSettingsConfig}
import io.hydrosphere.mist.master.models.RunMode
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.duration._
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
class LocalWorkerDriver(
  cluster: EndpointConfig,
  contextsSettings: ContextsSettings,
  logging: LogServiceConfig,
  sparkHome: String
) extends WorkerDriver with Logger {

  def run(settings: WorkerSettings): Unit = {
    import settings._

    val contextConfig = contextsSettings.configFor(context)
    val contextArg = contextConfig.sparkConf
      .map({case (k, v) => s"$k=$v"}).mkString(",")

    def durationToArg(d: Duration): String = d match {
      case f: FiniteDuration => s"${f.toSeconds}s"
      case _ => "Inf"
    }

    val cmd = Seq(
      s"${sys.env("MIST_HOME")}/bin/mist-worker",
      "--runner", "local2",
      "--master", s"${cluster.host}:${cluster.port}",
      "--name", name,
      "--context-name", context,
      "--spark-conf", contextArg,
      "--max-jobs", contextConfig.maxJobs.toString,
      "--downtime", durationToArg(contextConfig.downtime),
      "--spark-streaming-duration", durationToArg(contextConfig.streamingDuration),
      "--log-service", s"${logging.host}:${logging.port}",
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

trait WorkerRunner {

  def runWorker(name: String, context: String, mode: RunMode): Unit

  def onStop(name: String): Unit = {}
}

object WorkerRunner {

  def create(config: MasterConfig): WorkerRunner = {

    val driver = selectDriver(config)
    val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).toString

    new WorkerRunner {

      override def runWorker(name: String, context: String, mode: RunMode): Unit = {
        val settings = WorkerSettings(
          name = name,
          context = context,
          runOptions = config.contextsSettings.configFor(context).runOptions,
          //TODO
          configFilePath = "",
          jarPath = jarPath,
          mode = mode
        )
        driver.run(settings)
      }

      override def onStop(name: String): Unit = driver.onStop(name)
    }
  }

  def selectDriver(config: MasterConfig): WorkerDriver = {
    val runnerType = config.workers.runner
    runnerType match {
      case "local" =>
        sys.env.get("SPARK_HOME") match {
          case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for local runner")
          case Some(home) => new LocalWorkerDriver(config.cluster, config.contextsSettings, config.logs, home)
        }
      case "docker" => new DockerWorkerDriver(config.workers.dockerHost, config.workers.dockerPort)
      case "manual" => new ManualWorkerDriver(config.workers.cmd, config.workers.cmdStop)
      case _ =>
        throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")

    }
  }
}

