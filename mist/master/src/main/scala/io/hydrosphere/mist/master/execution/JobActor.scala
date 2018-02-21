package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, Terminated, Timers}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.execution.workers.WorkerConnection
import io.hydrosphere.mist.master.execution.status.StatusReporter
import mist.api.data.JsLikeData

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

sealed trait ExecStatus
object ExecStatus {
  case object Queued extends ExecStatus
  case object Started extends ExecStatus
  case object Cancelling extends ExecStatus
}

class JobActor(
  callback: ActorRef,
  req: RunJobRequest,
  promise: Promise[JsLikeData],
  report: StatusReporter
) extends Actor with ActorLogging with Timers {

  import JobActor._

  override def preStart(): Unit = {
    report.report(QueuedEvent(req.id))
    req.timeout match {
      case f: FiniteDuration =>
        timers.startSingleTimer(s"job-${req.id}", Event.Timeout, f)
      case _ =>
    }
  }

  override def receive: Receive = initial

  private def initial: Receive = {
    case Event.Cancel =>
      sender() ! JobIsCancelled(req.id)
      cancelFinally("user request")

    case Event.Timeout => cancelFinally("timeout")
    case Event.GetStatus => sender() ! ExecStatus.Queued

    case Event.Perform(connection) =>
      report.report(WorkerAssigned(req.id, connection.id))
      connection.ref ! req

      connection.whenTerminated.onComplete(_ => self ! Event.ConnectionTerminated)(context.dispatcher)
      context become starting(connection)
  }

  def cancelRemotely(cancelRespond: ActorRef, connection: WorkerConnection, reason: String): Unit = {
    log.info(s"Start cancelling ${req.id} remotely: $reason")
    connection.ref ! CancelJobRequest(req.id)
    context become cancellingOnExecutor(Seq(cancelRespond), connection, reason)
  }

  private def starting(connection: WorkerConnection): Receive = {
    case Event.GetStatus => sender() ! ExecStatus.Queued
    case Event.Cancel => cancelRemotely(sender(), connection, "user request")
    case Event.Timeout => cancelRemotely(sender(), connection, "timeout")

    case Event.ConnectionTerminated => onConnectionTermination()

    case JobFileDownloading(id, time) =>
      report.report(JobFileDownloadingEvent(id, time))

    case JobStarted(id, time) =>
      report.report(StartedEvent(id, time))
      context become completion(callback, connection)
  }

  private def completion(callback: ActorRef, connection: WorkerConnection): Receive = {
    case Event.GetStatus => sender() ! ExecStatus.Started
    case Event.Cancel => cancelRemotely(sender(), connection, "user request")
    case Event.Timeout => cancelRemotely(sender(), connection, "timeout")

    case Event.ConnectionTerminated => onConnectionTermination()

    case JobSuccess(_, data) => completeSuccess(data)
    case JobFailure(_, err) => completeFailure(err)
  }

  private def cancellingOnExecutor(cancelRespond: Seq[ActorRef], connection: WorkerConnection, reason: String): Receive = {
    case Event.GetStatus => sender() ! ExecStatus.Cancelling
    case Event.Cancel => context become cancellingOnExecutor(cancelRespond :+ sender(), connection, reason)
    case Event.Timeout => log.info(s"Timeout exceeded for ${req.id} that is in cancelling process")

    case Event.ConnectionTerminated =>
      val msg = akka.actor.Status.Failure(new IllegalStateException(s"Executor was terminated"))
      cancelRespond.foreach(_ ! msg)
      onConnectionTermination()

    case ev @ JobIsCancelled(_, _) =>
      cancelRespond.foreach(_ ! ev)
      cancelFinally(reason)

    case JobSuccess(_, data) =>
      val msg = akka.actor.Status.Failure(new IllegalStateException(s"Job ${req.id} was completed"))
      cancelRespond.foreach(_ ! msg)
      completeSuccess(data)

    case JobFailure(_, err) =>
      val msg = akka.actor.Status.Failure(new IllegalStateException(s"Job ${req.id} was completed"))
      cancelRespond.foreach(_ ! msg)
      completeFailure(err)
  }

  private def cancelFinally(
    reason: String,
    time: Long = System.currentTimeMillis()
  ): Unit = {
    promise.failure(new RuntimeException(s"Job was cancelled: $reason"))
    report.report(CanceledEvent(req.id, time))
    log.info(s"Job ${req.id} was cancelled: $reason")
    callback ! Event.Completed(req.id)
    self ! PoisonPill
  }

  private def onConnectionTermination(): Unit = completeFailure("Executor was terminated")

  private def completeSuccess(data: JsLikeData): Unit = {
    promise.success(data)
    report.report(FinishedEvent(req.id, System.currentTimeMillis(), data))
    log.info(s"Job ${req.id} completed successfully")
    callback ! Event.Completed(req.id)
    self ! PoisonPill
  }

  private def completeFailure(err: String): Unit = {
    promise.failure(new RuntimeException(err))
    report.report(FailedEvent(req.id, System.currentTimeMillis(), err))
    log.info(s"Job ${req.id} completed with error")
    callback ! Event.Completed(req.id)
    self ! PoisonPill
  }

}

object JobActor {

  sealed trait Event
  object Event {
    case object Cancel extends Event
    case object Timeout extends Event
    final case class Perform(connection: WorkerConnection) extends Event
    final case class Completed(id: String) extends Event
    case object GetStatus extends Event
    case object ConnectionTerminated extends Event
  }

  def props(
    callback: ActorRef,
    req: RunJobRequest,
    promise: Promise[JsLikeData],
    reporter: StatusReporter
  ): Props = Props(classOf[JobActor], callback, req, promise, reporter)

}

