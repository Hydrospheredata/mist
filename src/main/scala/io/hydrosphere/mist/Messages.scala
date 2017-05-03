package io.hydrosphere.mist

import akka.actor.{ActorRef, Address}
import io.hydrosphere.mist.Messages.JobMessages.RunJobRequest
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{Action, JobExecutionParams}
import io.hydrosphere.mist.master.models.RunMode

object Messages {

  object WorkerMessages {

    case class WorkerRegistration(name: String, adress: Address)
    case class WorkerCommand(name: String, message: Any)
    case class RunJobCommand(namespace: String, mode: RunMode, request: RunJobRequest)

    case class CreateContext(name: String)

    case object GetWorkers
    case object GetActiveJobs

    case class StopWorker(name: String)
    case object StopAllWorkers

    case class WorkerUp(ref: ActorRef)
    case object WorkerDown

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

    case class Register(id: String, params: JobExecutionParams, source: Source)

    sealed trait UpdateStatusEvent {
      val id: String
    }

    case class QueuedEvent(id: String) extends UpdateStatusEvent
    case class StartedEvent(id: String, time: Long) extends UpdateStatusEvent
    case class CanceledEvent(id: String, time: Long) extends UpdateStatusEvent
    case class FinishedEvent(id: String, time: Long, result: Map[String, Any]) extends UpdateStatusEvent
    case class FailedEvent(id: String, time: Long, error: String) extends UpdateStatusEvent

    // return full job details
    case object RunningJobs
    case class GetById(id: String)
    case class GetByExternalId(id: String)

  }

  // only for cli
  case object ListRoutes
}
