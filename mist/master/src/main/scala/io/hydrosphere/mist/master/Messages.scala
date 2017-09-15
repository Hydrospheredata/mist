package io.hydrosphere.mist.master

import akka.actor.ActorRef

import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.core.CommonData._

import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.models.{RunMode, ContextConfig}

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

    case class InitializedEvent(id: String, params: JobParams, externalId: Option[String]) extends UpdateStatusEvent
    case class QueuedEvent(id: String) extends UpdateStatusEvent
    case class StartedEvent(id: String, time: Long) extends UpdateStatusEvent
    case class JobFileDownloadingEvent(id: String, time: Long) extends UpdateStatusEvent
    case class CanceledEvent(id: String, time: Long) extends UpdateStatusEvent
    case class FinishedEvent(id: String, time: Long, result: Map[String, Any]) extends UpdateStatusEvent
    case class FailedEvent(id: String, time: Long, error: String) extends UpdateStatusEvent

    case class ReceivedLogs(id: String, events: Seq[LogEvent], fileOffset: Long) extends SystemEvent

    // return full job details
    case object RunningJobs
    case class GetHistory(limit: Int, offset: Int, statuses: Seq[JobDetails.Status])
    case class GetEndpointHistory(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status])
    case class GetById(id: String)
    case class RunningJobsByWorker(id: String)

  }

  object JobExecution {

    case class RunJobCommand(context: ContextConfig, mode: RunMode, request: RunJobRequest) {

      def computeWorkerId(): String = {
        val name = context.name
        mode match {
          case RunMode.Shared => name
          case RunMode.ExclusiveContext(id) =>
            val postfix = id.map(s => s"$s-${request.id}").getOrElse(request.id)
            s"$name-$postfix"
        }
      }
    }
    case class CancelJobCommand(workerId: String, request: CancelJobRequest)

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
