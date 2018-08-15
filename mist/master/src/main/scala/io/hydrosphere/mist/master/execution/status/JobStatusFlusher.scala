package io.hydrosphere.mist.master.execution.status

import akka.actor._
import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.JobDetails.Status
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.logging.JobLogger

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._

/**
  * Per job status updater
  */
class JobStatusFlusher(
  id: String,
  get: String => Future[JobDetails],
  update: JobDetails => Future[Unit],
  jobLogger: JobLogger
) extends Actor with ActorLogging {

  import JobStatusFlusher._

  implicit val ec = context.system.dispatcher

  override def preStart(): Unit = {
    context.setReceiveTimeout(30 seconds)
    get(id).onComplete {
      case Success(details) => self ! Ready(details)
      case Failure(e) =>
        log.error(e, "Failed to obtain initial state for {}", id)
        jobLogger.error(s"Failed to obtain initial state for $id", e)
        self ! PoisonPill
    }
  }

  override def receive: Receive = collect(Seq.empty)

  private def collect(messages: Seq[ReportedEvent]): Receive = {
    case e: ReportedEvent => context become collect(messages :+ e)
    case Ready(details) if messages.nonEmpty => performUpdate(details, messages)
    case Ready(details) => context become waitEvents(details)
  }

  private def waitEvents(details: JobDetails): Receive = {
    case e: ReportedEvent => performUpdate(details, Seq(e))
    case ReceiveTimeout => context stop self
  }

  private def performUpdate(current: JobDetails, messages: Seq[ReportedEvent]): Unit = {
    val next = messages.foldLeft(current) {
      case (d, re) =>
        logMessage(re.e)
        applyStatusEvent(d, re.e)
    }

    update(next).onComplete {
      case Success(_) =>
        messages.foreach({
          case ReportedEvent.FlushCallback(_, callback) => callback.success(next)
          case _ =>
        })
        self ! Ready(next)
      case Failure(e) =>
        messages.foreach({
          case ReportedEvent.FlushCallback(_, callback) => callback.failure(e)
          case _ =>
        })
        log.error(e, "Updating for {}, messages: {} was failed", id, messages)
        jobLogger.error(s"Updating for $id, messages: $messages was failed", e)
        self ! PoisonPill
    }
    context become collect(Seq.empty)
  }

  private def logMessage(evt: UpdateStatusEvent): Unit = evt match {
    case _: FailedEvent => jobLogger.error(show(evt))
    case _ => jobLogger.info(show(evt))
  }

  private def show(evt: UpdateStatusEvent): String = evt match {
    case _: FinishedEvent =>
      s"FinishedEvent"
    case _: StartedEvent =>
      s"StartedEvent"
    case _: CancellingEvent =>
      s"CancellingEvent"
    case _: CancelledEvent =>
      s"CanceledEvent"
    case _: JobFileDownloadingEvent =>
      s"JobFileDownloadingEvent"
    case _: QueuedEvent =>
      s"QueuedEvent"
    case InitializedEvent(_, _, extId, function, context) =>
      s"InitializedEvent(externalId=$extId)"
    case FailedEvent(_, _, err) =>
      s"FailedEvent with Error: \n $err"
    case WorkerAssigned(_, workerId) =>
      s"WorkerAssigned(workerId=$workerId)"
  }

}

object JobStatusFlusher {

  case class Ready(details: JobDetails)

  def props(
    id: String,
    get: String => Future[JobDetails],
    update: JobDetails => Future[Unit],
    loggerF: String => JobLogger
  ): Props = Props(classOf[JobStatusFlusher], id, get, update, loggerF(id))

  def applyStatusEvent(d: JobDetails, event: UpdateStatusEvent): JobDetails = {
    event match {
      case InitializedEvent(_, _, _, _, _) => d
      case QueuedEvent(_) => d.withStatus(Status.Queued)
      case StartedEvent(_, time) => d.withStartTime(time).withStatus(Status.Started)
      case WorkerAssigned(_, workerId) => d.copy(workerId = Some(workerId))
      case CancellingEvent(_, time) => d.withStatus(Status.Cancelling)
      case JobFileDownloadingEvent(_, _) => d.withStatus(Status.FileDownloading)
      case FinishedEvent(_, time, result) =>
        d.withEndTime(time).withJobResult(result).withStatus(Status.Finished)
      case FailedEvent(_, time, error) =>
        d.withStatus(Status.Failed).withEndTime(time).withFailure(error)
      case CancelledEvent(_, time) =>
        d.withEndTime(time).withStatus(Status.Canceled)
    }
  }
}
