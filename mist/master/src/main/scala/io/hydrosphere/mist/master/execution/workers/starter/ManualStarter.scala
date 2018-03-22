package io.hydrosphere.mist.master.execution.workers.starter

import java.nio.file.Path

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.ManualRunnerConfig
import io.hydrosphere.mist.utils.Logger

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
      "MIST_WORKER_SPARK_CONF" -> initInfo.sparkConf.map({case (k, v) => s"$k=$v"}).mkString("|+|")
    )
    val out = outDirectory.resolve(s"manual-worker-$name.log")
    val ps = WrappedProcess.run(start, env, out)
    if (async) NonLocal else Local(ps.await())
  }

  override def onStop(name: String): Unit = {
    stop.foreach(cmd => WrappedProcess.run(cmd, outDirectory.resolve(s"manual-worker-onstop-$name.log")))
  }
}

object ManualStarter {

  def apply(config: ManualRunnerConfig, outDirectory: Path): ManualStarter = {
    def split(s: String): Seq[String] = s.split(" ").map(_.trim).filter(_.nonEmpty)

    import config._
    ManualStarter(split(cmdStart), cmdStop.map(split), async, outDirectory)
  }
}
