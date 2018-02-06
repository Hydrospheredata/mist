package io.hydrosphere.mist.master

import akka.actor.ActorRef
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import mist.api.data.JsLikeData

object Messages {

  object StatusMessages {

    case class Register(
      request: RunJobRequest,
      endpoint: String,
      context: String,
      source: Source,
      externalId: Option[String],
      workerId: String
    )

    sealed trait SystemEvent
    sealed trait UpdateStatusEvent extends SystemEvent {
      val id: String
    }

    final case class InitializedEvent(id: String, params: JobParams, externalId: Option[String]) extends UpdateStatusEvent
    final case class QueuedEvent(id: String) extends UpdateStatusEvent
    final case class WorkerAssigned(id: String, workerId: String) extends UpdateStatusEvent
    final case class StartedEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class JobFileDownloadingEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class CanceledEvent(id: String, time: Long) extends UpdateStatusEvent
    final case class FinishedEvent(id: String, time: Long, result: JsLikeData) extends UpdateStatusEvent
    final case class FailedEvent(id: String, time: Long, error: String) extends UpdateStatusEvent

    final case class ReceivedLogs(id: String, events: Seq[LogEvent], fileOffset: Long) extends SystemEvent
    case object KeepAliveEvent extends SystemEvent

  }

  object JobExecution {

    case class RunJobCommand(context: ContextConfig, mode: RunMode, request: RunJobRequest)
    case class CancelJobCommand(context: String, request: CancelJobRequest)

    case class CreateContext(context: ContextConfig)

    case object GetWorkers
    case object GetActiveJobs
    case class FailRemainingJobs(reason: String)

    case class StopWorker(name: String)
    case object StopAllWorkers

    case class WorkerUp(ref: ActorRef)
    case object WorkerDown

    case class CheckWorkerUp(id: String)

    case class GetInitInfo(id: String)
  }

  // only for cli
  case object ListRoutes
  case class RunJobCli(endpointId: String, extId: Option[String], params: Map[String, Any])
}
