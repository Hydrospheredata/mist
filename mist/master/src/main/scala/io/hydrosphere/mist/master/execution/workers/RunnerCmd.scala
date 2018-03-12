package io.hydrosphere.mist.master.execution.workers

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.Logger
import cats.implicits._
import io.hydrosphere.mist.master.execution.workers

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success}

sealed trait WorkerProcess
case object NonLocal extends WorkerProcess
case class Local(termination: Future[Unit]) extends WorkerProcess

trait RunnerCommand2 {

  def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess

  def onStop(name: String): Unit = {}
}

object RunnerCommand2 {

  type MkProcessArgs = (String, WorkerInitInfo) => Seq[String]

  class WrappedProcess(ps: java.lang.Process) {

    def exitValue: Option[Int] = {
      try {
        ps.exitValue().some
      } catch {
        case _: IllegalThreadStateException => None
        case e: Throwable => throw e
      }
    }

    def await(step: FiniteDuration = 1 second): Future[Unit] = {
      val promise = Promise[Unit]
      val thread = new Thread(new Runnable {
        override def run(): Unit = {

          def loop(): Unit = {
            exitValue match {
              case Some(0) => promise.success(())
              case Some(x) =>
                val out = Source.fromInputStream(ps.getInputStream).getLines().take(25).mkString("; ")
                val errOut = Source.fromInputStream(ps.getErrorStream).getLines().take(25).mkString("; ")
                promise.failure(new RuntimeException(s"Process exited with status code $x and out: $out; errOut: $errOut"))
              case None =>
            }
          }

          while(!promise.isCompleted) {
            loop()
            Thread.sleep(step.toMillis)
          }
        }
      })
      thread.setDaemon(true)
      thread.start()
      promise.future
    }
  }

  object WrappedProcess {
    def run(cmd: Seq[String]): WrappedProcess = {
      import scala.collection.JavaConverters._
      val builder = new java.lang.ProcessBuilder(cmd.asJava)
      val ps = builder.start()
      new WrappedProcess(ps)
    }
  }

  class LocalCommand(f: MkProcessArgs) extends RunnerCommand2 with Logger {
    def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
      val cmd = f(name, initInfo)
      val ps = WrappedProcess.run(cmd)
      val future = ps.await()
      import scala.concurrent.ExecutionContext.Implicits.global
      future.onComplete({
        case Success(()) => logger.info("Completed normally")
        case Failure(e) => logger.error("SASAI", e)
      })
      Local(future)
    }
  }

  class SparkSubmit(mistHome: String, sparkHome: String, masterHost: String) extends RunnerCommand2 with Logger {
    override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
      def workerJarUrl: String = "http://" + initInfo.masterHttpConf + "/v2/api/artifacts_internal/mist-worker.jar"
      def workerLocalPath: String = Paths.get(mistHome, "mist-worker.jar").toRealPath().toString

      val submitPath = Paths.get(sparkHome, "bin", "spark-submit").toRealPath().toString
      val workerJar = if(initInfo.isK8S) workerJarUrl else workerLocalPath
      val conf = initInfo.sparkConf.flatMap({case (k, v) => Seq("--conf", s"$k=$v")})

      val runOpts = {
        val trimmed = initInfo.runOptions.trim
        if (trimmed.isEmpty) Seq.empty else trimmed.split(" ").map(_.trim).toSeq
      }

      val cmd = Seq(submitPath) ++ runOpts ++ conf ++ Seq(
        "--class", "io.hydrosphere.mist.worker.Worker",
        workerJar,
        "--master", masterHost,
        "--name", name
      )
      logger.info(s"Try submit worker $name, cmd: ${cmd.mkString(" ")}")
      val ps = WrappedProcess.run(cmd)
      val future = ps.await()
      Local(future)
    }
  }

  def create(masterAddress: String, workersSettings: WorkersSettingsConfig): RunnerCommand2 = {
    val runnerType = workersSettings.runner
    runnerType match {
      case "local" =>
        sys.env.get("SPARK_HOME") match {
          case None => throw new IllegalStateException("You should provide SPARK_HOME env variable for local runner")
          case Some(spH) => new workers.RunnerCommand2.SparkSubmit(
            sys.env.get("MIST_HOME").get,
            spH,
            masterAddress
          )
        }
      case _ => throw new IllegalArgumentException(s"Unknown worker runner type $runnerType")

    }
  }
}

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

