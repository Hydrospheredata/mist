package io.hydrosphere.mist.master.execution

import java.io.{PrintWriter, StringWriter}

import akka.actor.{Actor, ActorLogging, ActorRef, OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Timers}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection}
import io.hydrosphere.mist.master.logging.JobLogger
import mist.api.data.JsData

import scala.concurrent.Promise
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success}

sealed trait ExecStatus
object ExecStatus {
  case object Queued extends ExecStatus
  case object Started extends ExecStatus
  case object Cancelling extends ExecStatus
}

class JobActor(
  callback: ActorRef,
  req: RunJobRequest,
  promise: Promise[JsData],
  report: StatusReporter,
  jobLogger: JobLogger
) extends Actor with ActorLogging with Timers with FutureSubscribe {

  import JobActor._

  override def supervisorStrategy: SupervisorStrategy = SupervisorStrategy.stoppingStrategy

  val startTimeoutKey = s"job-start-${req.id}"
  val performTimeoutKey = s"job-perform-${req.id}"

  private def startTimeoutTimer(key: String, v: Duration): Unit = v match {
    case f: FiniteDuration => timers.startSingleTimer(startTimeoutKey, Event.Timeout, f)
    case _ =>
  }

  override def preStart(): Unit = {
    super.preStart()
    report.reportPlain(QueuedEvent(req.id))
    startTimeoutTimer(startTimeoutKey, req.startTimeout)
  }

  override def receive: Receive = initial(false)

  private def initial(startSoon: Boolean): Receive = {
    case Event.WorkerRequested if !startSoon =>
      jobLogger.info("Waiting worker connection")
      context become initial(true)
    case Event.WorkerRequested =>

    case Event.Cancel => cancelNotStarted("user request", Some(sender()))
    case Event.ContextBroken(e) => completeFailure(new RuntimeException("Context is broken", e), None)
    case Event.Timeout => cancelNotStarted(s"start timeout was exceeded: ${req.startTimeout}", None)

    case Event.GetStatus => sender() ! ExecStatus.Queued

    case Event.Perform(connection) =>
      timers.cancel(startTimeoutKey)
      startTimeoutTimer(performTimeoutKey, req.timeout)
      report.reportPlain(WorkerAssigned(req.id, connection.id))
      connection.run(req, self)

      subscribe0(connection.whenTerminated)(_ => Event.ConnectionTerminated, _ => Event.ConnectionTerminated)
      context become starting(connection)
  }

  def cancelRemotely(cancelRespond: ActorRef, connection: PerJobConnection, reason: String): Unit = {
    log.info(s"Start cancelling ${req.id} remotely: $reason")
    report.reportPlain(CancellingEvent(req.id, System.currentTimeMillis()))
    connection.cancel(req.id, self)
    context become cancellingOnExecutor(Seq(cancelRespond), connection, reason)
  }

  private def starting(connection: PerJobConnection): Receive = {
    case Event.GetStatus => sender() ! ExecStatus.Queued
    case Event.Cancel => cancelRemotely(sender(), connection, "user request")
    case Event.Timeout => cancelRemotely(sender(), connection, "timeout")

    case Event.ConnectionTerminated => onConnectionTermination(Some(connection))

    case JobFileDownloading(id, time) =>
      report.reportPlain(JobFileDownloadingEvent(id, time))

    case WorkerIsBusy =>
      log.warning("Connection is busy: unexpected case ¯\\_(ツ)_/¯")
      completeFailure("Connection is busy: unexpected case ¯\\_(ツ)_/¯", Some(connection))

    case JobStarted(id, time) =>
      report.reportPlain(StartedEvent(id, time))
      context become completion(callback, connection)

    case JobFailure(_, err) => completeFailure(err, Some(connection))
  }

  private def completion(callback: ActorRef, connection: PerJobConnection): Receive = {
    case Event.GetStatus => sender() ! ExecStatus.Started
    case Event.Cancel => cancelRemotely(sender(), connection, "user request")
    case Event.Timeout => cancelRemotely(sender(), connection, "timeout")

    case Event.ConnectionTerminated => onConnectionTermination(Some(connection))

    case JobSuccess(_, data) => completeSuccess(data, connection)
    case JobFailure(_, err) => completeFailure(err, Some(connection))
  }

  private def cancellingOnExecutor(cancelRespond: Seq[ActorRef], connection: PerJobConnection, reason: String): Receive = {
    case Event.GetStatus => sender() ! ExecStatus.Cancelling
    case Event.Cancel => context become cancellingOnExecutor(cancelRespond :+ sender(), connection, reason)
    case Event.Timeout => log.info(s"Timeout exceeded for ${req.id} that is in cancelling process")

    case Event.ConnectionTerminated =>
      val msg = akka.actor.Status.Failure(new IllegalStateException(s"Executor was terminated"))
      cancelRespond.foreach(_ ! msg)
      onConnectionTermination(Some(connection))

    case ev @ JobIsCancelled(_, time) => // do nothing wait succ/failure

    case JobSuccess(_, data) =>
      val msg = akka.actor.Status.Failure(new IllegalStateException(s"Job ${req.id} was completed"))
      cancelRespond.foreach(_ ! msg)
      completeSuccess(data, connection)

    case JobFailure(_, err) =>
      cancelStarted(reason, cancelRespond, Some(connection), err)
  }

  private def cancelNotStarted(err: Throwable, respond: Option[ActorRef]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val time = System.currentTimeMillis()
    jobLogger.warn(err.getMessage)
    promise.failure(err)
    report.reportPlain(CancellingEvent(req.id, time))
    report.reportWithFlushCallback(CancelledEvent(req.id, time)).onComplete(t => {
      val response = t match {
        case Success(d) => ContextEvent.JobCancelledResponse(req.id, d)
        case Failure(e) => akka.actor.Status.Failure(e)
      }
      respond.foreach(_ ! response)
    })
    log.info(s"Job ${req.id} was cancelled: ${err.getMessage}")
    callback ! Event.Completed(req.id)
    context.stop(self)
  }

  private def cancelNotStarted(reason: String, respond: Option[ActorRef]): Unit =
    cancelNotStarted(new RuntimeException(s"Job was cancelled: $reason"), respond)

  private def cancelStarted(
    reason: String,
    respond: Seq[ActorRef],
    maybeConn: Option[PerJobConnection],
    error: String
  ): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val time = System.currentTimeMillis()
    promise.failure(new RuntimeException(s"Job was cancelled: $reason"))
    report.reportWithFlushCallback(CancelledEvent(req.id, time)).onComplete(t => {
      val response = t match {
        case Success(d) => ContextEvent.JobCancelledResponse(req.id, d)
        case Failure(e) => akka.actor.Status.Failure(e)
      }
      respond.foreach(_ ! response)
    })
    log.info(s"Job ${req.id} was cancelled: $reason")
    maybeConn.foreach(_.release())
    callback ! Event.Completed(req.id)
    context.stop(self)
  }

  private def onConnectionTermination(maybeConn: Option[PerJobConnection]): Unit =
    completeFailure("Executor was terminated", maybeConn)

  private def completeSuccess(data: JsData, connection: PerJobConnection): Unit = {
    promise.success(data)
    report.reportPlain(FinishedEvent(req.id, System.currentTimeMillis(), data))
    log.info(s"Job ${req.id} completed successfully")
    callback ! Event.Completed(req.id)
    connection.release()
    context.stop(self)
  }

  private def completeFailure(err: Throwable, maybeConn: Option[PerJobConnection]): Unit = {
    val sw = new StringWriter()
    err.printStackTrace(new PrintWriter(sw))
    val errMessage = sw.toString
    completeFailure(errMessage, maybeConn)
  }


  private def completeFailure(err: String, maybeConn: Option[PerJobConnection]): Unit = {
    promise.failure(new RuntimeException(err))
    report.reportPlain(FailedEvent(req.id, System.currentTimeMillis(), err))
    log.info(s"Job ${req.id} completed with error")
    maybeConn.foreach(_.release())
    callback ! Event.Completed(req.id)
    context.stop(self)
  }

}

object JobActor {

  sealed trait Event
  object Event {
    case object WorkerRequested extends Event
    case object Cancel extends Event
    case class ContextBroken(e: Throwable) extends Event
    case object Timeout extends Event
    final case class Perform(conn: PerJobConnection) extends Event
    final case class Completed(id: String) extends Event
    case object GetStatus extends Event
    case object ConnectionTerminated extends Event
  }

  def props(
    callback: ActorRef,
    req: RunJobRequest,
    promise: Promise[JsData],
    reporter: StatusReporter,
    jobLogger: JobLogger
  ): Props = Props(classOf[JobActor], callback, req, promise, reporter, jobLogger)

}

