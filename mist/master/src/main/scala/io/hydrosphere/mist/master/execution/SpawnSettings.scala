package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.workers.RunnerCmd
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.duration._

case class SpawnSettings(
  runner: RunnerCmd,
  timeout: Duration,
  readyTimeout: FiniteDuration,
  akkaAddress: String,
  logAddress: String,
  httpAddress: String,
  maxArtifactSize: Long
) {

  def toWorkerInitInfo(ctx: ContextConfig): WorkerInitInfo =
    WorkerInitInfo(
      sparkConf = ctx.sparkConf,
      maxJobs = ctx.maxJobsOnNode,
      downtime = ctx.downtime,
      streamingDuration = ctx.streamingDuration,
      logService = this.logAddress,
      masterHttpConf = this.httpAddress,
      maxArtifactSize = this.maxArtifactSize
    )
}

