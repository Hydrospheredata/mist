package io.hydrosphere.mist.master.execution.workers

import java.io.File

import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._

trait WorkerRunner {

  def runWorker(name: String, context: ContextConfig): Unit

  def onStop(name: String): Unit = {}
}

trait ShellWorkerScript {

  def workerArgs(
    name: String,
    context: ContextConfig,
    config: MasterConfig
  ): Seq[String] = {

    Seq[String](
      "--master", s"${config.cluster.host}:${config.cluster.port}",
      "--name", name
    ) ++ mkRunOptions(context)
  }

  def mkRunOptions(ctxConfig: ContextConfig): Seq[String] = {
    val opts = ctxConfig.runOptions
    if (opts.isEmpty)
      Seq.empty
    else
      Seq("--run-options", opts)
  }

  def durationToArg(d: Duration): String = d match {
    case f: FiniteDuration => s"${f.toSeconds}s"
    case _ => "Inf"
  }

}

object ShellWorkerScript extends ShellWorkerScript

/**
  * Spawn workers on the same host
  */
class LocalWorkerRunner(config: MasterConfig)
  extends WorkerRunner with ShellWorkerScript with Logger {

  override def runWorker(name: String, context: ContextConfig): Unit = {
    val cmd =
      Seq[String](s"${sys.env("MIST_HOME")}/bin/mist-worker", "--runner", "local") ++
      workerArgs(name, context, config)

    logger.info(s"Try run local worker with $cmd")
    val builder = Process(cmd)
    builder.run(false)
  }

}

class DockerWorkerRunner(config: MasterConfig)
  extends WorkerRunner with ShellWorkerScript {

  override def runWorker(name: String, context: ContextConfig): Unit = {
    val cmd =
      Seq(s"${sys.env("MIST_HOME")}/bin/mist-worker",
          "--runner", "docker",
          "--docker-host", config.workers.dockerHost,
          "--docker-port", config.workers.dockerPort.toString) ++
      workerArgs(name, context, config)
    val builder = Process(cmd)
    builder.run(false)
  }

}

/**
  * Run worker via user-provided shell script
  * For example use in case when we need to do something before actually starting worker
  * <pre>
  * <code>
  *   #!/bin/bash
  *   # do smth and then run worker
  *   bin/mist-worker --runner local\
  *         --master \u0024MIST_MASTER_ADDRESS\
  *         --name \u0024MIST_WORKER_NAME\
  * </code>
  * </pre>
  */
class ManualWorkerRunner(
  config: MasterConfig,
  jarPath: String) extends WorkerRunner {

  override def runWorker(name: String, context: ContextConfig): Unit = {
    Process(
      Seq("bash", "-c", config.workers.cmd),
      None,
      "MIST_MASTER_ADDRESS" -> s"${config.cluster.host}:${config.cluster.port}",
      "MIST_WORKER_NAME" -> name,
      "MIST_WORKER_RUN_OPTIONS" -> context.runOptions,

      "MIST_WORKER_JAR_PATH" -> jarPath
    ).run(false)
  }

  override def onStop(name: String): Unit = withStopCommand { cmd =>
    Process(
      Seq("bash", "-c", cmd),
      None,
      "MIST_WORKER_NAME" -> name
    ).run(false)
  }

  private def withStopCommand(f: String => Unit): Unit = {
    val cmd = config.workers.cmdStop
    if (cmd.nonEmpty) f(cmd)
  }

}


object WorkerRunner {

  def create(config: MasterConfig): WorkerRunner = {
    val runnerType = config.workers.runner
    runnerType match {
      case "local" =>
        sys.env.get("SPARK_HOME") match {
          case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for local runner")
          case Some(home) => new LocalWorkerRunner(config)
        }
      case "docker" => new DockerWorkerRunner(config)
      case "manual" =>
        val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).toString
        new ManualWorkerRunner(config, jarPath)
      case _ =>
        throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")

    }
  }
}

