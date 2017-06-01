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
import scala.util.{Failure, Success}

class StatusService(
  store: JobRepository,
  notifiers: Seq[JobEventPublisher]
) extends Actor with ActorLogging {

  val activeStatuses = List(Status.Queued, Status.Running, Status.Initialized)

  override def receive: Receive = {
    case Register(req, endpoint, ctx, source, externalId) =>
      val details = JobDetails(endpoint, req.id, req.params, ctx, externalId, source)
      val s = sender()
      store.update(details).map(_ => {
        s ! akka.actor.Status.Success(())
        val event = InitializedEvent(req.id, req.params)
        callNotifiers(event)
      })

    case x: UpdateStatusEvent =>
      handleUpdateStatus(x)
      callNotifiers(x)

    case RunningJobs =>
      store.filteredByStatuses(activeStatuses) pipeTo sender()

    case GetById(id) =>
      store.get(id) pipeTo sender()

    case GetByExternalId(id) =>
      store.getByExternalId(id) pipeTo sender()

    case GetEndpointHistory(id) =>
      store.getByEndpointId(id) pipeTo sender()
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

  private def callNotifiers(event: UpdateStatusEvent): Unit =
    notifiers.foreach(n => n.notify(event))

}

object StatusService {

  def props(store: JobRepository, notifiers: Seq[JobEventPublisher]): Props =
    Props(classOf[StatusService], store, notifiers)

  def applyStatusEvent(d: JobDetails, event: UpdateStatusEvent): JobDetails = {
    event match {
       //TODO
      case InitializedEvent(_, _) => d
      case QueuedEvent(_) => d.withStatus(Status.Queued)
      case StartedEvent(_, time) => d.withStartTime(time).withStatus(Status.Running)
      case CanceledEvent(_, time) => d.withEndTime(time).withStatus(Status.Aborted)
      case FinishedEvent(_, time, result) =>
        d.withEndTime(time).withJobResult(result).withStatus(Status.Stopped)
      case FailedEvent(_, time, error) =>
        if (d.status == Status.Aborted)
          d
        else
          d.withEndTime(time).withStatus(Status.Error).withFailure(error)
    }
  }
}
