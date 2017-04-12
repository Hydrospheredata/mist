package io.hydrosphere.mist.master

import java.io.File

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

  import scala.sys.process._

  def run(settings: WorkerSettings): Unit = {
    val jarPath = getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
    val cmd = Seq(
      s"${sys.env("MIST_HOME")}/bin/mist",
      "start",
      "worker",
      "--runner", "local",
      "--namespace", settings.name,
      "--config", settings.configFilePath,
      "--jar", jarPath.toString,
      "--run-options", settings.runOptions)

    val builder = Process(cmd)
    builder.run(true)
  }

}

//class LocalWorkerRunner(sparkHome: String) extends WorkerRunner with Logger {
//
//  import scala.sys.process._
//
//  def run(settings: WorkerSettings): Unit = {
//    val jarPath = getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath
//
//    logger.info(sys.env.getOrElse("PYTHONPATH", "NP PYTHON PATH"))
//    val current = sys.env.get("PYTHONPATH").map(p => ":" + p).getOrElse("")
//    val env = Array(
//      "PYTHONPATH=/home/vadim/projects/mist/spark_local/spark-1.5.2-bin-hadoop2.6/python:/home/vadim/projects/mist/spark_local/spark-1.5.2-bin-hadoop2.6/python/lib/py4j-0.8.2.1.src.zip" + current
//    )
//    val cmd = Seq(
//      s"bin/mist",
//      "start",
//      "worker",
//      "--runner", "local",
//      "--namespace", settings.name,
//      "--config", settings.configFilePath,
//      "--jar", jarPath.toString,
//      "--run-options", settings.runOptions
//    )
//    logger.info(s"cmd $cmd")
//
//    new Thread(new Runnable {
//      override def run(): Unit = cmd.!!(new ProcessLogger {
//        override def buffer[T](f: => T): T = f
//
//        override def out(s: => String): Unit = logger.info(s)
//
//        override def err(s: => String): Unit = logger.error(s)
//      })
//    }).start()
////    val cmd = Array(
////      "/bin/sh", "-c",
////      s"$sparkHome/bin/spark-submit --class io.hydrosphere.mist.worker.Worker" +
////        s""" --driver-java-options "-Dconfig.file=${settings.configFilePath} -Dakka.cluster.roles.1=worker-${settings.name}" """ +
////        s" ${jarPath.toString} ${settings.name} >/dev/null 2>&1 &"
////    )
////
////    logger.info(s"Try starting worker with cmd ${cmd.mkString(" ")} ${env.mkString(" ")}")
////    val ps = Runtime.getRuntime.exec(cmd, env)
//  }

//}

object WorkerRunners {

}
