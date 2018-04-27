package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Path

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.workers.StopAction
import io.hydrosphere.mist.utils.Logger

import scala.util.{Failure, Success}

class LocalSparkSubmit(builder: SparkSubmitBuilder, outDirectory: Path) extends WorkerStarter with Logger {

  override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
    val cmd = builder.submitWorker(name, initInfo)
    logger.info(s"Try submit local worker $name, cmd: ${cmd.mkString(" ")}")
    val out = outDirectory.resolve(s"local-worker-$name.log")

    WrappedProcess.run(cmd, out) match {
      case Success(ps) => WorkerProcess.Local(ps.await())
      case Failure(e) => WorkerProcess.Failed(e)
    }
  }

  override def stopAction: StopAction = StopAction.Remote
}

object LocalSparkSubmit {

  def apply(outDirectory: Path, mistHome: String, sparkHome: String): LocalSparkSubmit =
    new LocalSparkSubmit(SparkSubmitBuilder(mistHome, sparkHome), outDirectory)

  def apply(outDirectory: Path): LocalSparkSubmit = (sys.env.get("MIST_HOME"), sys.env.get("SPARK_HOME")) match {
    case (Some(mHome), Some(spHome)) => LocalSparkSubmit(outDirectory, mHome, spHome)
    case (mist, spark) =>
      def inverse[A](opt: Option[A], value: A): Option[A] = opt match {
        case Some(_) => None
        case None => Some(value)
      }
      val errors = Seq(inverse(mist, "MIST_HOME"), inverse(spark, "SPARK_HOME")).flatten.mkString(",")
      val msg = s"Missing system environment variables: $errors"
      throw new IllegalStateException(msg)
  }
}

