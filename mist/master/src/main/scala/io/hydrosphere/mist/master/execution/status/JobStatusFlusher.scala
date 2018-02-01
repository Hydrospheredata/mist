package io.hydrosphere.mist.master.execution.status

import akka.actor._
import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.JobDetails.Status
import io.hydrosphere.mist.master.Messages.StatusMessages._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

/**
  * Per job status updater
  */
class JobStatusFlusher(
  id: String,
  get: String => Future[JobDetails],
  update: JobDetails => Future[Unit]
) extends Actor with ActorLogging {

  import JobStatusFlusher._
  implicit val ec = context.system.dispatcher

  override def preStart(): Unit = {
    context.setReceiveTimeout(30 seconds)
    get(id).onComplete {
      case Success(details) => self ! Ready(details)
      case Failure(e) =>
        log.error(e, "Failed to obtain initial state for {}", id)
        self ! PoisonPill
    }
  }

  override def receive: Receive = collect(Seq.empty)

  private def collect(messages: Seq[UpdateStatusEvent]): Receive = {
    case e: UpdateStatusEvent => context become collect(messages :+ e)
    case Ready(details) if messages.nonEmpty => performUpdate(details, messages)
    case Ready(details) => context become waitEvents(details)
  }

  private def waitEvents(details: JobDetails): Receive = {
    case e: UpdateStatusEvent => performUpdate(details, Seq(e))
    case ReceiveTimeout => context stop self
  }

  private def performUpdate(current: JobDetails, messages: Seq[UpdateStatusEvent]): Unit = {
    val next = messages.foldLeft(current)({case (d,e) => applyStatusEvent(d, e)})
    update(next).onComplete {
      case Success(_) => self ! Ready(next)
      case Failure(e) =>
        log.error(e, "Updating for {}, messages: {} was failed", id, messages)
        self ! PoisonPill
    }
    context become collect(Seq.empty)
  }

}

object JobStatusFlusher {
  case class Ready(details: JobDetails)

  def props(
    id: String,
    get: String => Future[JobDetails],
    update: JobDetails => Future[Unit]
  ): Props = Props(classOf[JobStatusFlusher], id, get, update)

  def applyStatusEvent(d: JobDetails, event: UpdateStatusEvent): JobDetails = {
    event match {
      case InitializedEvent(_, _, _) => d
      case QueuedEvent(_) => d.withStatus(Status.Queued)
      case StartedEvent(_, time) => d.withStartTime(time).withStatus(Status.Started)
      case CanceledEvent(_, time) => d.withEndTime(time).withStatus(Status.Canceled)
      case JobFileDownloadingEvent(_, _) => d.withStatus(Status.FileDownloading)
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
