package io.hydrosphere.mist.master

import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.JobDetails.Source
import mist.api.data.JsData

object Messages {

  object StatusMessages {

    case class Register(
      request: RunJobRequest,
      function: String,
      context: String,
      source: Source,
      externalId: Option[String],
      workerId: String
    )

    sealed trait SystemEvent
    sealed trait UpdateStatusEvent extends SystemEvent {
      val id: String
    }

    final case class InitializedEvent(id: String, params: JobParams, externalId: Option[String], function: String, context: String) extends UpdateStatusEvent
    final case class QueuedEvent(id: String) extends UpdateStatusEvent
    final case class WorkerAssigned(id: String, workerId: String) extends UpdateStatusEvent
    final case class StartedEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class JobFileDownloadingEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class CancellingEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class CancelledEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class FinishedEvent(id: String, time: Long, result: JsData) extends UpdateStatusEvent
    final case class FailedEvent(id: String, time: Long, error: String) extends UpdateStatusEvent

    final case class ReceivedLogs(id: String, events: Seq[LogEvent], fileOffset: Long) extends SystemEvent
    case object KeepAliveEvent extends SystemEvent

  }

}
