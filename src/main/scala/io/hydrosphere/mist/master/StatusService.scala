package io.hydrosphere.mist.master

import akka.pattern._
import akka.actor.{Actor, ActorLogging, Props}
import io.hydrosphere.mist.Messages.JobMessages.JobStarted
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.master.store.JobRepository
import StatusService._

import scala.concurrent.ExecutionContext.Implicits.global

class StatusService(store: JobRepository) extends Actor with ActorLogging {

  val activeStatuses = List(Status.Queued, Status.Running, Status.Initialized)

  override def receive: Receive = {
    case Register(id, params, source) =>
      val details = JobDetails(params, source, id)
      store.update(details)

    case x:UpdateStatusEvent => handleUpdateStatus(x)

    case RunningJobs =>
      store.filteredByStatuses(activeStatuses) pipeTo sender()

    case GetById(id) =>
      store.get(id) pipeTo sender()

    case GetByExternalId(id) =>
      store.getByExternalId(id) pipeTo sender()
  }

  private def handleUpdateStatus(e: UpdateStatusEvent): Unit = {
    store.get(e.id).map({
      case Some(d) =>
        val updated = applyStatusEvent(d, e)
        store.update(updated)
      case None =>
        log.warning(s"Received $e for unknown job")
    })
  }

}

object StatusService {

  def props(store: JobRepository): Props =
    Props(classOf[StatusService], store)

  def applyStatusEvent(d: JobDetails, event: UpdateStatusEvent): JobDetails = {
    event match {
      case QueuedEvent(_) => d.withStatus(Status.Queued)
      case StartedEvent(_, time) => d.withStartTime(time).withStatus(Status.Running)
      case CanceledEvent(_, time) => d.withEndTime(time).withStatus(Status.Aborted)
      case FinishedEvent(_, time, result) =>
        d.withEndTime(time).withJobResult(Left(result)).withStatus(Status.Stopped)
      case FailedEvent(_, time, error) =>
        if (d.status == Status.Aborted)
          d
        else
          d.withEndTime(time).withStatus(Status.Error).withJobResult(Right(error))
    }
  }
}
