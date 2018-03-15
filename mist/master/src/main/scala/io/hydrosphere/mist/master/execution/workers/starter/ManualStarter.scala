package io.hydrosphere.mist.master.execution.workers.starter

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.ManualRunnerConfig
import io.hydrosphere.mist.utils.Logger

case class ManualStarter(
  start: Seq[String],
  stop: Option[Seq[String]],
  async: Boolean
) extends WorkerStarter with Logger {

  override def onStart(name: String, initInfo: WorkerInitInfo): WorkerProcess = {
    val env = Map(
      "MIST_MASTER_ADDRESS" -> initInfo.masterAddress,
      "MIST_WORKER_NAME" -> name,
      "MIST_WORKER_RUN_OPTIONS" -> initInfo.runOptions,
      "MIST_WORKER_SPARK_CONF" -> initInfo.sparkConf.map({case (k, v) => s"$k=$v"}).mkString("|+|")
    )
    val ps = WrappedProcess.run(start, env)
    if (async) NonLocal else Local(ps.await())
  }

  override def onStop(name: String): Unit = {
    stop.foreach(WrappedProcess.run)
  }
}

object ManualStarter {

  def apply(config: ManualRunnerConfig): ManualStarter = {
    def split(s: String): Seq[String] = s.split(" ").map(_.trim).filter(_.nonEmpty)

    import config._
    ManualStarter(split(cmdStart), cmdStop.map(split), async)
  }
}
