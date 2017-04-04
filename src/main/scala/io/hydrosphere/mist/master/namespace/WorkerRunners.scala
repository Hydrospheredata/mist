package io.hydrosphere.mist.master.namespace

import java.io.File
import java.nio.file.Paths

import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{MistConfig, Worker}

import scala.language.postfixOps

case class WorkerSettings(
  name: String,
  runOptions: String,
  configFilePath: String,
  jarPath: String
)

trait WorkerRunner {

  def run(settings: WorkerSettings): Unit

  def start(settings: WorkerSettings): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = WorkerRunner.this.run(settings)
    }).start()
  }
}

class NewWorkerRunner(sparkHome: String) extends WorkerRunner with Logger {

  import scala.sys.process._

  override def run(settings: WorkerSettings): Unit = {
    val jarPath = getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
    val cmd = Seq(
      Paths.get(sparkHome, "bin", "spark-submit").toString,
      "--class", "io.hydrosphere.mist.Worker2",
      "--driver-java-options",
      s"""-Dconfig.file=${settings.configFilePath} -Dakka.cluster.roles.1=worker-${settings.name}""",
      jarPath.toString,
      settings.name
    )
    logger.info(s"Try starting worker with cmd $cmd")
    val code = cmd.!(new ProcessLogger {
      override def buffer[T](f: => T): T = f

      override def out(s: => String): Unit = ()

      override def err(s: => String): Unit = ()
    })
    logger.info(s"Worker ${settings.name} is stopped with code $code")
  }
}

object WorkerRunners {

  import scala.sys.process._

  val SameProcess = new WorkerRunner {
    override def run(settings: WorkerSettings): Unit =
      Worker.main(Array(settings.name))
  }

  val Local = new WorkerRunner {
    override def run(settings: WorkerSettings): Unit = {
      val cmd: Seq[String] = Seq(
        s"${sys.env("MIST_HOME")}/bin/mist",
        "start",
        "worker",
        "--runner", "local",
        "--namespace", settings.name,
        "--config", settings.configFilePath,
        "--jar", settings.jarPath,
        "--run-options", settings.runOptions)
      cmd !
    }
  }

  val Docker = new WorkerRunner {
    override def run(settings: WorkerSettings): Unit = {
      val cmd: Seq[String] = Seq(
        s"${sys.env("MIST_HOME")}/bin/mist",
        "start",
        "worker",
        "--runner", "docker",
        "--docker-host", MistConfig.Workers.dockerHost,
        "--docker-port", MistConfig.Workers.dockerPort.toString,
        "--namespace", settings.name,
        "--config", settings.configFilePath,
        "--jar", settings.configFilePath,
        "--run-options", settings.runOptions)
      cmd !
    }
  }


  val Manual = new WorkerRunner {
    override def run(settings: WorkerSettings): Unit = {
      Process(
        Seq("bash", "-c", MistConfig.Workers.cmd),
        None,
        "MIST_WORKER_NAMESPACE" -> settings.name,
        "MIST_WORKER_CONFIG" -> settings.configFilePath,
        "MIST_WORKER_JAR_PATH" -> settings.jarPath,
        "MIST_WORKER_RUN_OPTIONS" -> settings.runOptions
      ).!
    }
  }
}
