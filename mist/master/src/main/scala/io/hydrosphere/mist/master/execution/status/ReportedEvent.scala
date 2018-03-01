package io.hydrosphere.mist.master.execution.status

import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.Messages.StatusMessages.UpdateStatusEvent

import scala.concurrent.Promise

sealed trait ReportedEvent {
  val e: UpdateStatusEvent
}

object ReportedEvent {

  final case class PlainEvent(e: UpdateStatusEvent) extends ReportedEvent
  final case class FlushCallback(e: UpdateStatusEvent, callback: Promise[JobDetails]) extends ReportedEvent

  def plain(e: UpdateStatusEvent): ReportedEvent = PlainEvent(e)
  def withCallback(e: UpdateStatusEvent): FlushCallback = FlushCallback(e, Promise[JobDetails])

}

