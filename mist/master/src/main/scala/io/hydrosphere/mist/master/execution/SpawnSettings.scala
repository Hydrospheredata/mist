package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.workers.WorkerRunner
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.duration.Duration

case class SpawnSettings(
  runner: WorkerRunner,
  timeout: Duration,
  akkaAddress: String,
  logAddress: String,
  httpAddress: String,
  maxArtifactSize: Long,
  jobsSavePath: String
) {

  def toWorkerInitInfo(ctx: ContextConfig): WorkerInitInfo =
    WorkerInitInfo(
      sparkConf = ctx.sparkConf,
      maxJobs = ctx.maxJobsOnNode,
      downtime = ctx.downtime,
      streamingDuration = ctx.streamingDuration,
      logService = this.logAddress,
      masterHttpConf = this.httpAddress,
      maxArtifactSize = this.maxArtifactSize,
      jobsSavePath = this.jobsSavePath
    )
}

