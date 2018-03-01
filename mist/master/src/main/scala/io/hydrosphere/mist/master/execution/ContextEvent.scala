package io.hydrosphere.mist.master.execution

import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future

sealed trait ContextEvent
object ContextEvent {
  final case class RunJobCommand(context: ContextConfig, request: RunJobRequest) extends ContextEvent
  final case class CancelJobCommand(context: String, request: CancelJobRequest) extends ContextEvent
  final case class UpdateContext(context: ContextConfig) extends ContextEvent

  final case class JobCancelledResponse(id: String, details: JobDetails) extends ContextEvent
}
