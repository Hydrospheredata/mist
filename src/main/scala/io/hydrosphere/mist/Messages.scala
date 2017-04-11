package io.hydrosphere.mist

import akka.actor.{ActorRef, ActorSelection, Address}
import io.hydrosphere.mist.jobs.Action

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

  // only for cli
  case object ListRoutes
}
