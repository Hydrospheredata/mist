package io.hydrosphere.mist.master

import io.hydrosphere.mist.utils.Logger

import scala.language.postfixOps

case class WorkerSettings(
  name: String,
  runOptions: String,
  configFilePath: String,
  jarPath: String
)

trait WorkerRunner {

  def run(settings: WorkerSettings): Unit
}

/**
  *
  */
class LocalWorkerRunner(sparkHome: String) extends WorkerRunner with Logger {

  def run(settings: WorkerSettings): Unit = {
    val jarPath = getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath

    val cmd = Array(
      "/bin/sh", "-c",
      sparkHome + "/bin/spark-submit" +
      " --class io.hydrosphere.mist.worker.Worker" +
      " --driver-java-options" +
      " \"-Dconfig.file=" + settings.configFilePath + " -Dakka.cluster.roles.1=worker-" + settings.name + "\"" +
      " " + jarPath.toString +
      " " + settings.name //+
      //" >/dev/null 2>&1 &"
    )

    logger.info(s"Try starting worker with cmd ${cmd.mkString(" ")}")
    Runtime.getRuntime.exec(cmd)
  }

}

object WorkerRunners {

}
