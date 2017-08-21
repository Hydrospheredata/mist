package io.hydrosphere.mist.master

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern._
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.master.StatusService._
import io.hydrosphere.mist.master.logging.JobsLogger
import io.hydrosphere.mist.master.store.JobRepository

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class StatusService(
  store: JobRepository,
  notifiers: Seq[JobEventPublisher],
  jobsLogger: JobsLogger
) extends Actor with ActorLogging {

  val activeStatuses = List(Status.Queued, Status.Started, Status.Initialized)

  override def receive: Receive = {
    case Register(req, endpoint, ctx, source, externalId, workerId) =>
      val details = JobDetails(endpoint, req.id, req.params, ctx, externalId, source, workerId = workerId)
      val s = sender()
      jobsLogger.info(req.id, s"Register $details")
      store.update(details).map(_ => {
        s ! akka.actor.Status.Success(())
        val event = InitializedEvent(req.id, req.params, externalId)
        callNotifiers(event)
      })

    case e: UpdateStatusEvent =>
      val origin = sender()
      updateDetails(e).onComplete({
        case Success(Some(details)) =>
          jobsLogger.info(e.id, s"$e for job $details")
          origin ! details
          callNotifiers(e)

        case Success(None) =>
          val message = s"Received $e for unknown job"
          log.warning(message)
          jobsLogger.warn(e.id, message)
        case Failure(err) =>
          jobsLogger.error(e.id, s"Update job $e failed", err)
          log.error(err, "Update job details with {} failed", e)
      })

    case RunningJobs =>
      store.filteredByStatuses(activeStatuses) pipeTo sender()

    case GetById(id) =>
      store.get(id) pipeTo sender()

    case GetEndpointHistory(id, limit, offset, statuses) =>
      store.getByEndpointId(id, limit, offset, statuses) pipeTo sender()

    case GetHistory(limit, offset, statuses) =>
      store.getAll(limit, offset, statuses) pipeTo sender()
  }

  private def updateDetails(e: UpdateStatusEvent): Future[Option[JobDetails]] = {
    val result = for {
      details <- OptionT(store.get(e.id))
      updated = applyStatusEvent(details, e)
      _ <- OptionT.liftF(store.update(updated))
    } yield updated

    result.value
  }

  private def callNotifiers(event: UpdateStatusEvent): Unit =
    notifiers.foreach(n => n.notify(event))

}

object StatusService {

  def props(store: JobRepository, notifiers: Seq[JobEventPublisher], jobsLogger: JobsLogger): Props =
    Props(classOf[StatusService], store, notifiers, jobsLogger)

  def applyStatusEvent(d: JobDetails, event: UpdateStatusEvent): JobDetails = {
    event match {
      case InitializedEvent(_, _, _) => d
      case QueuedEvent(_) => d.withStatus(Status.Queued)
      case StartedEvent(_, time) => d.withStartTime(time).withStatus(Status.Started)
      case CanceledEvent(_, time) => d.withEndTime(time).withStatus(Status.Canceled)
      case FinishedEvent(_, time, result) =>
        d.withEndTime(time).withJobResult(result).withStatus(Status.Finished)
      case FailedEvent(_, time, error) =>
        if (d.status == Status.Canceled)
          d
        else
          d.withEndTime(time).withStatus(Status.Failed).withFailure(error)
    }
  }
}
