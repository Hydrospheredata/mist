package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorLogging, Props}
import io.hydrosphere.mist.Messages.JobMessages.JobStarted
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.master.store.JobRepository
import StatusService._

class StatusService(store: JobRepository) extends Actor with ActorLogging {

  val activeStatuses = List(Status.Queued, Status.Running, Status.Initialized)

  override def receive: Receive = {
    case JobStarted(id, time) =>
     actualDetails(id).foreach(d => {
       val updated = d.withStartTime(time)
       store.update(updated)
     })

    case Register(id, params, source) =>
      val details = JobDetails(params, source, id)
      store.update(details)

    case x:UpdateStatusEvent => handleUpdateStatus(x)

    case RunningJobs =>
      sender() ! store.filteredByStatuses(activeStatuses)

    case GetById(id) =>
      sender() ! store.get(id)

    //TODO: should be list
    case GetByExternalId(id) =>
      sender() ! store.getByExternalId(id)
  }

  private def handleUpdateStatus(e: UpdateStatusEvent): Unit = {
    actualDetails(e.id) match {
      case Some(d) =>
        val updated = applyStatusEvent(d, e)
        store.update(updated)
      case None =>
        log.warning(s"Received $e for unknown job")
    }
  }

  private def actualDetails(id: String): Option[JobDetails] = store.get(id)
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
