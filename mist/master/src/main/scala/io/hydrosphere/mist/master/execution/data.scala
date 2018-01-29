package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.{JobResult, WorkerRunner}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.Duration

sealed trait ExecState
object ExecState {
  case object Queued extends ExecState
  case object Started extends ExecState
}

case class JobStatus(
  request: RunJobRequest,
  state: ExecState,
  result: Promise[JobResult]
)

case class SpawnSettings(
  runner: WorkerRunner,
  timeout: Duration
)

