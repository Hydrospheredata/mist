package io.hydrosphere.mist

import akka.actor.{ActorRef, ActorSelection, Address}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobExecutionParams}

object Messages {

  object WorkerMessages {

    case class WorkerRegistration(name: String, adress: Address)
    case class WorkerCommand(name: String, message: Any)

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
    case class UpdateStatus(id: String, status: JobDetails.Status, time: Long)
    // return full job details
    case object RunningJobs

    sealed trait GetStatus
    case class GetStatusById(id: String) extends GetStatus
    case class GetStatusByExternalId(id: String) extends GetStatus

  }

  // only for cli
  case object ListRoutes
}
