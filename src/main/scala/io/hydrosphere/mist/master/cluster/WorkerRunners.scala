package io.hydrosphere.mist.master.cluster

import scala.language.postfixOps
import io.hydrosphere.mist.{MistConfig, Worker}

case class WorkerSettings(
  name: String,
  runOptions: String,
  configFilePath: String,
  jarPath: String
)

trait WorkerRunner {

  protected def run(settings: WorkerSettings): Unit

  def start(settings: WorkerSettings): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = WorkerRunner.this.run(settings)
    }).start()
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
