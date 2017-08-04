package io.hydrosphere.mist

import akka.actor.{ActorRef, Address}
import io.hydrosphere.mist.Messages.JobMessages.{CancelJobRequest, JobParams, RunJobRequest}
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{Action, JobDetails}
import io.hydrosphere.mist.master.models.RunMode

object Messages {

  object WorkerMessages {

    case class WorkerRegistration(name: String, address: Address)
    case class RunJobCommand(context: String, mode: RunMode, request: RunJobRequest)
    case class CancelJobCommand(workerId: String, request: CancelJobRequest)

    case class CreateContext(contextId: String)

    case object GetWorkers
    case object GetActiveJobs
    case class FailRemainingJobs(reason: String)

    case class StopWorker(name: String)
    case object StopAllWorkers

    case class WorkerUp(ref: ActorRef)
    case object WorkerDown

    case class CheckWorkerUp(id: String)

  }

  object JobMessages {

    case class RunJobRequest(
      id: String,
      params: JobParams
    )

    case class JobParams(
      filePath: String,
      className: String,
      arguments: Map[String, Any],
      action: Action
    )

    sealed trait RunJobResponse {
      val id: String
      val time: Long
    }

    case class JobStarted(
      id: String,
      time: Long = System.currentTimeMillis()
    ) extends RunJobResponse

    case class WorkerIsBusy(
      id: String,
      time: Long = System.currentTimeMillis()
    ) extends RunJobResponse


    case class CancelJobRequest(id: String)
    case class JobIsCancelled(
      id: String,
      time: Long = System.currentTimeMillis()
    )

    // internal messages
    sealed trait JobResponse {
      val id: String
    }

    case class JobSuccess(id: String, result: Map[String, Any]) extends JobResponse
    case class JobFailure(id: String, error: String) extends JobResponse

  }

  object StatusMessages {

    case class Register(
      request: RunJobRequest,
      endpoint: String,
      context: String,
      source: Source,
      externalId: Option[String])

    sealed trait SystemEvent
    sealed trait UpdateStatusEvent extends SystemEvent {
      val id: String
    }

    case class InitializedEvent(id: String, params: JobParams, externalId: Option[String]) extends UpdateStatusEvent
    case class QueuedEvent(id: String, workerId: String) extends UpdateStatusEvent
    case class StartedEvent(id: String, time: Long) extends UpdateStatusEvent
    case class CanceledEvent(id: String, time: Long) extends UpdateStatusEvent
    case class FinishedEvent(id: String, time: Long, result: Map[String, Any]) extends UpdateStatusEvent
    case class FailedEvent(id: String, time: Long, error: String) extends UpdateStatusEvent

    case class ReceivedLogs(id: String, events: Seq[LogEvent], fileOffset: Long) extends SystemEvent

    // return full job details
    case object RunningJobs
    case class GetHistory(limit: Int, offset: Int, statuses: Seq[JobDetails.Status])
    case class GetEndpointHistory(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status])
    case class GetById(id: String)

  }

  // only for cli
  case object ListRoutes
  case class RunJobCli(endpointId: String, extId: Option[String], params: Map[String, Any])
}
