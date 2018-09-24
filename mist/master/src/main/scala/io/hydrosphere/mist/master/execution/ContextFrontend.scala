package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import io.hydrosphere.mist.common.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.Messages.StatusMessages.FailedEvent
import io.hydrosphere.mist.master.execution.ContextFrontend.Event.JobDied
import io.hydrosphere.mist.master.execution.ContextFrontend.{ConnectorState, FrontendStatus}
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnector}
import io.hydrosphere.mist.master.logging.{JobLogger, JobLoggersFactory, LogService}
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax}
import mist.api.data.JsData

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait FrontendBasics {

  type State = FrontendState[String, ActorRef]
  val State = FrontendState

  def mkStatus(state: State, failures: Int, execId: Option[String]): FrontendStatus = {
    val jobs =
      state.queued.map({case (k, _) => k -> ExecStatus.Queued}) ++
      state.active.map({case (k, _) => k -> ExecStatus.Started})

    FrontendStatus(jobs, failures, execId)
  }

  def mkStatus(state: State): FrontendStatus = mkStatus(state, 0, None)
  def mkStatus(state: State, conn: ConnectorState): FrontendStatus = mkStatus(state, conn.failedTimes, Option(conn.id))
}


class ContextFrontend(
  name: String,
  reporter: StatusReporter,
  loggersFactory: JobLoggersFactory,
  connectorStarter: (String, ContextConfig) => Future[WorkerConnector],
  jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsData], StatusReporter, JobLogger)],
  defaultInactiveTimeout: FiniteDuration
) extends Actor
  with ActorLogging
  with ActorFSyntax
  with FrontendBasics
  with Timers {

  import ContextFrontend._
  import context.dispatcher

  override def receive: Receive = initial

  private def initial: Receive = {
    case ContextEvent.UpdateContext(ctx) =>
      becomeAwaitOrConnected(ctx, State.empty)
  }

  private def awaitRequest(ctx: ContextConfig, timerKey: String): Receive = {
    case Event.Status => sender() ! FrontendStatus.empty
    case ContextEvent.UpdateContext(updCtx) =>
      becomeAwaitOrConnected(updCtx, State.empty)

    case req: RunJobRequest =>
      timers.cancel(timerKey)
      val next = mkJob(req, State.empty, sender())
      gotoStartingConnector(ctx, next, 0)

    case Event.Downtime =>
      log.info(s"Context $name was inactive")
      context stop self
  }

  // handle UpdateContext for awaitRequest/initial
  private def becomeAwaitOrConnected(ctx: ContextConfig, state: State): Unit = {
    if (ctx.precreated && ctx.workerMode == RunMode.Shared) {
      gotoStartingConnector(ctx, state, 0)
    } else {
      val timerKey = s"$name-await-timeout"
      timers.startSingleTimer(timerKey, Event.Downtime, defaultInactiveTimeout)
      context become awaitRequest(ctx, timerKey)
    }
  }

  private def shouldGoToEmptyWithTimeout(state: State, ctx: ContextConfig): Boolean = {
    state.isEmpty && !ctx.precreated && ctx.downtime.isFinite()
  }

  // handle currentState changes, starting new jobs if it's possible
  // or awaiting Downtime timeout
  private def becomeWithConnector(
    ctx: ContextConfig,
    state: State,
    connectorState: ConnectorState
  ): Unit = {

    def askConnection(): Unit = {
      connectorState.connector.askConnection().onComplete {
        case Success(connection) => self ! Event.Connection(connectorState.id, connection)
        case Failure(e) => self ! Event.ConnectionFailure(connectorState.id, e)
      }
    }

    if (shouldGoToEmptyWithTimeout(state, ctx)) {
      gotoEmptyWithConnector(ctx, connectorState)
    } else {
      val available = ctx.maxJobs - connectorState.all
      val need = math.min(state.queued.size - connectorState.asked, available)
      val nextConnState = {
        if (need > 0) {
          for (job <- state.takeNext(need)) {
            askConnection()
            job ! JobActor.Event.WorkerRequested
          }
          connectorState.copy(asked = connectorState.asked + need)
        } else {
          connectorState
        }
      }
      log.info("Context {} - connected state(active connections: {}, max: {})", name, connectorState.all, ctx.maxJobs)
      context become withConnector(ctx, state, nextConnState)
    }
  }

  private def withConnector(
    ctx: ContextConfig,
    currentState: State,
    connectorState: ConnectorState
  ): Receive = {

    def becomeNextState(next: State): Unit = becomeWithConnector(ctx, next, connectorState)
    def becomeNextConn(next: ConnectorState): Unit = becomeWithConnector(ctx, currentState, next)
    def becomeNext(c: ConnectorState, s: State): Unit = becomeWithConnector(ctx, s, c)


    {
      case Event.Status => sender() ! mkStatus(currentState, connectorState)
      case ContextEvent.UpdateContext(updCtx) =>
        connectorState.connector.shutdown(false)
        gotoStartingConnector(updCtx, currentState, 0)

      case req: RunJobRequest => becomeNextState(mkJob(req, currentState, sender()))
      case CancelJobRequest(id) =>
        cancelJob(id, currentState, sender())

      case Event.Connection(connId, connection) if connId == connectorState.id =>
        log.info("Context {} received new connection", name)
        currentState.nextOption match {
          case Some((id, ref)) =>
            ref ! JobActor.Event.Perform(connection)
            becomeNext(connectorState.askSuccess, currentState.toWorking(id))
          case None =>
            connection.release()
            becomeNextConn(connectorState.askSuccess.connectionReleased)
        }

      case Event.Connection(_, connection) => connection.release()

      case Event.ConnectionFailure(connId, e) if connId == connectorState.id =>
        log.error(e, "Ask new worker connection for {} failed", name)
        val newConnState = connectorState.askFailure
        if (newConnState.failedTimes >= ctx.maxConnFailures) {
          connectorState.connector.shutdown(false)
          becomeSleeping(currentState, ctx, e, newConnState.failedTimes)
        } else {
          becomeNextConn(newConnState)
        }

      case JobActor.Event.Completed(id) =>
        val (conns, state) = currentState.getWithState(id) match {
          case Some((_, Working)) => connectorState.connectionReleased -> currentState.done(id)
          case Some((_, Waiting)) => connectorState -> currentState.done(id)
          case None => connectorState -> currentState
        }
        becomeNext(conns, state)

      case Event.ConnectorCrushed(id, e) if id == connectorState.id =>
        log.error(e, "Context {} - connector {} was crushed", name, id)
        val next = connectorState.connectorFailed
        if (next.failedTimes >= ctx.maxConnFailures) {
          becomeSleeping(currentState, ctx, e, next.failedTimes)
        } else {
          gotoStartingConnector(ctx, currentState, next.failedTimes)
        }

      //TODO is it possible to stop connector outside except ContextFrontend???
      case Event.ConnectorStopped(id) if id == connectorState.id =>
        log.info("Context {} - connector {} was stopped", name, id)
        gotoStartingConnector(ctx, currentState, 0)

      case Event.ConnectorStarted(_, conn) => conn.shutdown(true)
    }
  }

  private def gotoEmptyWithConnector(
    ctx: ContextConfig,
    connectorState: ConnectorState
  ): Unit = {
    val timerKey = s"$name-wait-downtime"
    val timeout = ctx.downtime match {
      case f: FiniteDuration => f
      case _ => defaultInactiveTimeout
    }
    timers.startSingleTimer(timerKey, Event.Downtime, timeout)
    log.info("Context {} - move to inactive state", name)
    context become emptyWithConnector(ctx, connectorState, timerKey)
  }

  // optional state - use it if ctx isn't precreated
  private def emptyWithConnector(
    ctx: ContextConfig,
    connectorState: ConnectorState,
    timerKey: String
  ): Receive = {
    case Event.Status => sender() ! mkStatus(State.empty[String, ActorRef], connectorState)
    case ContextEvent.UpdateContext(updCtx) =>
      connectorState.connector.shutdown(false)
      gotoStartingConnector(updCtx, State.empty, 0)

    case req: RunJobRequest =>
      timers.cancel(timerKey)
      val next = mkJob(req, State.empty, sender())
      becomeWithConnector(ctx, next, connectorState)

    case Event.Connection(connId, connection) if connId == connectorState.id =>
      connection.release()
      val next = connectorState.askSuccess.connectionReleased
      context become emptyWithConnector(ctx, next, timerKey)

    case Event.Connection(_, connection) =>
      connection.release()

    case Event.Downtime =>
      log.info(s"Context $name was inactive")
      connectorState.connector.shutdown(false)
      context stop self

    case Event.ConnectorCrushed(id, e) if id == connectorState.id =>
      log.error(e, "Context {} was crushed in empty state - shutdown", name)
      context stop self

    case Event.ConnectorStopped(id) if id == connectorState.id =>
      log.info("Context {} was stopped in empty state - shutdown", name)
      context stop self

    case Event.ConnectorStarted(_, conn) => conn.shutdown(true)
  }

  def becomeSleeping(
    state: State,
    brokenCtx: ContextConfig,
    error: Throwable,
    failedTimes: Int
  ): Unit = {
    state.queued.foreach({case (_, ref) => ref ! JobActor.Event.ContextBroken(error)})
    context become sleepingTilUpdate(state, brokenCtx, error, failedTimes)
  }

  private def sleepingTilUpdate(
    state: State,
    brokenCtx: ContextConfig,
    error: Throwable,
    failedTimes: Int
  ): Receive = {
    case Event.Status => sender() ! mkStatus(state, failedTimes, None)
    case ContextEvent.UpdateContext(updCtx) => becomeAwaitOrConnected(updCtx, FrontendState.empty)

    case req: RunJobRequest => respondWithError(brokenCtx, req, sender(), error)

    case CancelJobRequest(id) =>
      cancelJob(id, state, sender())

    case Event.Connection(_, connection) => connection.release()

    case JobActor.Event.Completed(id) =>
      val next = state.get(id) match {
        case Some(_) => state.done(id)
        case None => state
      }
      context become sleepingTilUpdate(next, brokenCtx, error, failedTimes)

    case Event.ConnectorStarted(_, conn) => conn.shutdown(true)
  }

  private def startingConnector(
    ctx: ContextConfig,
    state: State,
    connId: String,
    failedTimes: Int
  ): Receive = {
    case Event.Status => sender() ! mkStatus(state)

    // TODO check if it possible to update current connector/cluster or start new
    case ContextEvent.UpdateContext(updCtx) =>
      log.info(s"Update ctx: $name")
      gotoStartingConnector(updCtx, state, 0)

    case req: RunJobRequest =>
      val next = mkJob(req, state, sender())
      context become startingConnector(ctx, next, connId, failedTimes)

    case CancelJobRequest(id) => cancelJob(id, state, sender())

    case JobActor.Event.Completed(id) =>
      val next = state.get(id) match {
        case Some(_) => state.done(id)
        case None => state
      }
      context become startingConnector(ctx, next, connId, failedTimes)

    case Event.Connection(_, connection) => connection.release()

    case Event.ConnectorStarted(id, conn) if id == connId =>
      log.info("Connector started {} id: {}", name, id)
      if (ctx.precreated && ctx.workerMode == RunMode.Shared) conn.warmUp()
      val connState = ConnectorState.initial(id, conn).copy(failedTimes = failedTimes)
      conn.whenTerminated().onComplete({
        case Success(_) => self ! Event.ConnectorStopped(id)
        case Failure(e) => self ! Event.ConnectorCrushed(id, e)
      })

      if (shouldGoToEmptyWithTimeout(state, ctx)) {
        gotoEmptyWithConnector(ctx, connState)
      } else {
        becomeWithConnector(ctx, state, connState)
      }


    case Event.ConnectorStarted(_, conn) => conn.shutdown(true)
    case Event.ConnectorStartFailed(id, e) if id == connId =>
      log.error(e, "Starting connector {} id: {} failed", name, id)
      if (failedTimes > ctx.maxConnFailures) {
        becomeSleeping(state, ctx, e, failedTimes)
      } else {
        gotoStartingConnector(ctx, state, failedTimes + 1)
      }
  }

  private def gotoStartingConnector(ctx: ContextConfig, state: State, failedTimes: Int): Unit = {
    val id = ctx.name + "_" + UUID.randomUUID().toString
    log.info(s"Starting connector $id for $name")
    connectorStarter(id, ctx).onComplete {
      case Success(connector) => self ! Event.ConnectorStarted(id, connector)
      case Failure(e) => self ! Event.ConnectorStartFailed(id, e)
    }
    context become startingConnector(ctx, state, id, failedTimes)
  }

  //TODO bug - it's possible to cancel and then assign connection to job
  private def cancelJob(id: String, state: State, respond: ActorRef): Unit = state.get(id) match {
    case Some(ref) =>
      ref.tell(JobActor.Event.Cancel, respond)
    case None =>
      respond ! akka.actor.Status.Failure(new IllegalArgumentException(s"Unknown job: $id"))
  }

  private def mkJob(req: RunJobRequest, st: State, respond: ActorRef): State = {
    val promise = Promise[JsData]
    val ref = jobFactory.create(self, req, promise, reporter, loggersFactory.getJobLogger(req.id))
    context.watchWith(ref, JobDied(req.id))

    respond ! ExecutionInfo(req, promise)
    st.enqueue(req.id, ref)
  }

  private def respondWithError(brokenCtx: ContextConfig, req: RunJobRequest, respond: ActorRef, error: Throwable): Unit = {
    val msg = buildErrorMessage(
      s"Please update context ${brokenCtx.name} before running function ${req.params.className} invocation",
      error
    )
    val promise = Promise[JsData].failure(error)
    reporter.reportPlain(FailedEvent(req.id, System.currentTimeMillis(), msg))
    respond ! ExecutionInfo(req, promise)
  }

  private def buildErrorMessage(prefix: String, err: Throwable): String = {
    val msg = Option(err.getMessage).getOrElse("")
    val trace = err.getStackTrace.map(e => e.toString).mkString(";\n")
    val suffix = s"Type: ${err.getClass.getCanonicalName}, message: $msg, trace \n$trace"
    s"$prefix: $suffix"
  }

}

object ContextFrontend {

  sealed trait Event
  object Event {
    final case class ConnectorStarted(id: String, connector: WorkerConnector) extends Event
    final case class ConnectorStartFailed(id: String, e: Throwable) extends Event
    final case class ConnectorCrushed(id: String, err: Throwable) extends Event
    final case class ConnectorStopped(id: String) extends Event

    final case class JobDied(id: String) extends Event
    final case class JobCompleted(id: String) extends Event

    final case class Connection(connectorId: String, conn: PerJobConnection) extends Event
    final case class ConnectionFailure(connectorId: String, e: Throwable) extends Event

    case object Downtime extends Event
    case object Status extends Event
  }

  case class FrontendStatus(
    jobs: Map[String, ExecStatus],
    failures: Int,
    executorId: Option[String]
  )
  object FrontendStatus {
    val empty: FrontendStatus = FrontendStatus(Map.empty, 0, None)
  }

  case class ConnectorState(
    id: String,
    connector: WorkerConnector,
    used: Int,
    asked: Int,
    failedTimes: Int
  ) {

    def all: Int = used + asked
    def askSuccess: ConnectorState = copy(used = used + 1, asked = asked - 1, failedTimes = 0)
    def askFailure: ConnectorState = copy(asked = asked - 1, failedTimes = failedTimes + 1)
    def connectionReleased: ConnectorState = copy(used = used - 1)
    def connectorFailed: ConnectorState = copy(failedTimes = failedTimes + 1)
  }

  object ConnectorState {
    def initial(id: String, connector: WorkerConnector): ConnectorState =
      ConnectorState(id, connector, 0, 0, 0)
  }

  def props(
    name: String,
    status: StatusReporter,
    loggersFactory: JobLoggersFactory,
    connectorStarter: (String, ContextConfig) => Future[WorkerConnector],
    jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsData], StatusReporter, JobLogger)],
    defaultInactiveTimeout: FiniteDuration
  ): Props = Props(classOf[ContextFrontend], name, status, loggersFactory, connectorStarter, jobFactory, defaultInactiveTimeout)


  def props(
    name: String,
    status: StatusReporter,
    loggersFactory: JobLoggersFactory,
    connectorStarter: (String, ContextConfig) => Future[WorkerConnector]
  ): Props = props(name, status, loggersFactory, connectorStarter, ActorF.props(JobActor.props _), 5 minutes)
}
