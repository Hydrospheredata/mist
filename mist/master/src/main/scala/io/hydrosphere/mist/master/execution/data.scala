package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.{JobResult, WorkerRunner}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration

sealed trait ExecStatus
object ExecStatus {
  case object Queued extends ExecStatus
  case object Started extends ExecStatus
  case object Cancelling extends ExecStatus
}

case class JobStatus(
  request: RunJobRequest,
  state: ExecStatus,
  result: Promise[JobResult]
)

case class SpawnSettings(
  runner: WorkerRunner,
  timeout: Duration
)

