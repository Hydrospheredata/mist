package io.hydrosphere.mist.master.namespace

import akka.actor.{ActorSelection, Address}
import io.hydrosphere.mist.jobs.Action

object WorkerMessages {

  case class WorkerRegistration(name: String, adress: Address)
  case class WorkerCommand(name: String, message: Any)

  case object GetWorkers
  case class StopWorker(name: String)

  case class WorkerUp(ref: ActorSelection)
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
