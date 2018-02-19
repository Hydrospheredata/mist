package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process._

trait RunnerCmd {

  def runWorker(name: String, context: ContextConfig): Unit

  def onStop(name: String): Unit = {}
}

trait ShellWorkerScript {

  def workerArgs(
    name: String,
    context: ContextConfig,
    masterAddress: String
  ): Seq[String] = {

    Seq[String](
      "--master", masterAddress,
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
class LocalRunnerCmd(masterAddress: String)
  extends RunnerCmd with ShellWorkerScript with Logger {

  override def runWorker(name: String, context: ContextConfig): Unit = {
    val cmd =
      Seq[String](s"${sys.env("MIST_HOME")}/bin/mist-worker", "--runner", "local") ++
      workerArgs(name, context, masterAddress)

    logger.info(s"Try run local worker with $cmd")
    val builder = Process(cmd)
    builder.run(false)
  }

}

class DockerRunnerCmd(masterAddress: String, dockerHost: String, dockerPort: Int)
  extends RunnerCmd with ShellWorkerScript {

  override def runWorker(name: String, context: ContextConfig): Unit = {
    val cmd =
      Seq(s"${sys.env("MIST_HOME")}/bin/mist-worker",
          "--runner", "docker",
          "--docker-host", dockerHost,
          "--docker-port", dockerPort.toString) ++
      workerArgs(name, context, masterAddress)
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
class ManualRunnerCmd(masterAddress: String, cmdStart: String, cmdStop: String) extends RunnerCmd {

  override def runWorker(name: String, context: ContextConfig): Unit = {
    Process(
      Seq("bash", "-c", cmdStart),
      None,
      "MIST_MASTER_ADDRESS" -> masterAddress,
      "MIST_WORKER_NAME" -> name,
      "MIST_WORKER_RUN_OPTIONS" -> context.runOptions
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
    if (cmdStop.nonEmpty) f(cmdStop)
  }

}


object RunnerCmd {

  def create(masterAddress: String, workersSettings: WorkersSettingsConfig): RunnerCmd = {
    val runnerType = workersSettings.runner
    runnerType match {
      case "local" =>
        sys.env.get("SPARK_HOME") match {
          case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for local runner")
          case Some(_) => new LocalRunnerCmd(masterAddress)
        }
      case "docker" => new DockerRunnerCmd(masterAddress, workersSettings.dockerHost, workersSettings.dockerPort)
      case "manual" => new ManualRunnerCmd(masterAddress, workersSettings.cmd, workersSettings.cmdStop)
      case _ => throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")

    }
  }
}

