package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Path

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.ManualRunnerConfig
import io.hydrosphere.mist.master.execution.workers.StopAction
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

case class ManualStarter(
  start: Seq[String],
  stop: Option[Seq[String]],
  async: Boolean,
  outDirectory: Path
) extends WorkerStarter with Logger {

  override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
    val env = Map(
      "MIST_MASTER_ADDRESS" -> initInfo.masterAddress,
      "MIST_WORKER_NAME" -> name,
      "MIST_WORKER_RUN_OPTIONS" -> initInfo.runOptions,
      "MIST_WORKER_SPARK_CONF" -> initInfo.sparkConf.map({case (k, v) => s"$k=$v"}).mkString("|+|"),
      "MIST_WORKER_SPARK_CONF_PREPARED" -> initInfo.sparkConf.map({case (k, v) => s"--conf $k=$v"}).mkString(" ")
    )
    val out = outDirectory.resolve(s"manual-worker-$name.log")
    WrappedProcess.run(start, env, out) match {
      case Success(ps) => if (async) WorkerProcess.NonLocal else WorkerProcess.Local(ps.await())
      case Failure(e) => WorkerProcess.Failed(e)
    }

  }

  override def stopAction: StopAction = stop match {
    case Some(cmd) => StopAction.CustomFn(id => {
      val env = Map("MIST_WORKER_NAME" -> id)
      WrappedProcess.run(cmd, env, outDirectory.resolve(s"manual-worker-stop-$id.log")) match {
        case Success(ps) => ps.await().failed.foreach(e => logger.error(s"Calling stop script failed for $id", e))
        case Failure(e) => logger.error("Calling stop script failed for $id", e)
      }
    })
    case None => StopAction.Remote
  }
}

object ManualStarter {

  import PsUtil._

  def apply(config: ManualRunnerConfig, outDirectory: Path): ManualStarter = {
    import config._
    ManualStarter(parseArguments(cmdStart), cmdStop.map(parseArguments), async, outDirectory)
  }
}
