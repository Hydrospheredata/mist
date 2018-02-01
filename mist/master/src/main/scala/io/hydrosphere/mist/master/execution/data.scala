package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.master.WorkerRunner

import scala.concurrent.duration.Duration

sealed trait ExecStatus
object ExecStatus {
  case object Queued extends ExecStatus
  case object Started extends ExecStatus
  case object Cancelling extends ExecStatus
}

case class SpawnSettings(
  runner: WorkerRunner,
  timeout: Duration
)

