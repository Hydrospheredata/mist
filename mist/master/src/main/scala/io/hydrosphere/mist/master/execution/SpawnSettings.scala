package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.WorkerInitInfo
import io.hydrosphere.mist.master.execution.workers.{RunnerCmd, RunnerCommand2}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.duration._

case class SpawnSettings(
  runnerCmd: RunnerCommand2,
  timeout: Duration,
  readyTimeout: FiniteDuration,
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
      jobsSavePath = this.jobsSavePath,
      runOptions = ctx.runOptions,
      isK8s = ctx.isK8s
    )
}

