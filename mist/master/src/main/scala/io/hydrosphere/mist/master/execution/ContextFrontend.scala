package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.Messages.StatusMessages.FailedEvent
import io.hydrosphere.mist.master.execution.ContextFrontend.Event.JobDied
import io.hydrosphere.mist.master.execution.ContextFrontend.{ConnectorState, FrontendStatus}
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnector}
import io.hydrosphere.mist.master.logging.{JobLogger, JobLoggersFactory, LogService}
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax}
import mist.api.data.JsData

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait FrontendBasics {

  type State = FrontendState[String, ActorRef]
  val State = FrontendState

  def mkStatus(state: State, conn: Option[ConnectorState]): FrontendStatus = {
    val jobs =
      state.queued.map({case (k, _) => k -> ExecStatus.Queued}) ++
      state.active.map({case (k, _) => k -> ExecStatus.Started})

    FrontendStatus(
      jobs = jobs,
      conn.map(_.id),
      conn.map(_.failedTimes).getOrElse(0)
    )
  }

  def mkStatus(state: State): FrontendStatus = mkStatus(state, None)
  def mkStatus(state: State, conn: ConnectorState): FrontendStatus = mkStatus(state, Some(conn))
}

class ContextFrontend(
  name: String,
  reporter: StatusReporter,
  loggersFactory: JobLoggersFactory,
  connectorStarter: (String, ContextConfig) => WorkerConnector,
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
      val (id, connector) = startConnector(ctx)
      becomeWithConnector(ctx, next, ConnectorState.initial(id, connector))

    case Event.Downtime =>
      log.info(s"Context $name was inactive")
      context stop self
  }

  // handle UpdateContext for awaitRequest/initial
  private def becomeAwaitOrConnected(ctx: ContextConfig, state: State): Unit = {
    if (ctx.precreated && ctx.workerMode == RunMode.Shared) {
      val (id, connector) = startConnector(ctx)
      becomeWithConnector(ctx, state, ConnectorState.initial(id, connector))
    } else {
      val timerKey = s"$name-await-timeout"
      timers.startSingleTimer(timerKey, Event.Downtime, defaultInactiveTimeout)
      context become awaitRequest(ctx, timerKey)
    }
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

    def shouldGoToEmptyWithTimeout(): Boolean = {
      state.isEmpty && !ctx.precreated && ctx.downtime.isFinite()
    }

    if (shouldGoToEmptyWithTimeout()) {
      val timerKey = s"$name-wait-downtime"
      val timeout = ctx.downtime match {
        case f: FiniteDuration => f
        case _ => defaultInactiveTimeout
      }
      timers.startSingleTimer(timerKey, Event.Downtime, timeout)
      log.info("Context {} - move to inactive state", name)
      context become emptyWithConnector(ctx, connectorState, timerKey)
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

    def becomeSleeping(state: State, conn: ConnectorState, brokenCtx: ContextConfig, error: Throwable): Unit = {
      currentState.queued.foreach({case (_, ref) => ref ! JobActor.Event.ContextBroken(error)})
      context become sleepingTilUpdate(state, conn, brokenCtx, error)
    }

    {
      case Event.Status => sender() ! mkStatus(currentState, connectorState)
      case ContextEvent.UpdateContext(updCtx) =>
        connectorState.connector.shutdown(false)
        val (newId, newConn) = startConnector(updCtx)
        becomeWithConnector(updCtx, currentState, ConnectorState.initial(newId, newConn))

      case req: RunJobRequest => becomeNextState(mkJob(req, currentState, sender()))
      case CancelJobRequest(id) => becomeNextState(cancelJob(id, currentState, sender()))

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
          becomeSleeping(currentState, newConnState, ctx, e)
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
          becomeSleeping(currentState, next, ctx, e)
        } else {
          val (newId, newConn) = startConnector(ctx)
          becomeWithConnector(ctx, currentState, ConnectorState.initial(newId, newConn).copy(failedTimes = next.failedTimes))
        }

      case Event.ConnectorStopped(id) if id == connectorState.id =>
        log.info("Context {} - connector {} was stopped", name, id)
        val (newId, newConn) = startConnector(ctx)
        becomeWithConnector(ctx, currentState, ConnectorState.initial(newId, newConn))
    }
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
      val (newId, newConn) = startConnector(updCtx)
      context become emptyWithConnector(updCtx, ConnectorState.initial(newId, newConn), timerKey)

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
  }

  private def sleepingTilUpdate(state: State, conn: ConnectorState, brokenCtx: ContextConfig, error: Throwable): Receive = {
    case Event.Status => sender() ! mkStatus(state, conn)
    case ContextEvent.UpdateContext(updCtx) => becomeAwaitOrConnected(updCtx, FrontendState.empty)

    case req: RunJobRequest => respondWithError(brokenCtx, req, sender(), error)

    case CancelJobRequest(id) =>
      val next = cancelJob(id, state, sender())
      context become sleepingTilUpdate(next, conn, brokenCtx, error)

    case Event.Connection(_, connection) => connection.release()

    case JobActor.Event.Completed(id) =>
      val (conns, next) = state.getWithState(id) match {
        case Some((_, Working)) => conn.connectionReleased -> state.done(id)
        case Some((_, Waiting)) => conn -> state.done(id)
        case None => conn -> state
      }
      context become sleepingTilUpdate(next, conns, brokenCtx, error)
  }

  private def startConnector(ctx: ContextConfig): (String, WorkerConnector) = {
    val id = ctx.name + "_" + UUID.randomUUID().toString
    log.info(s"Starting executor $id for $name")
    val connector = connectorStarter(id, ctx)
    if (ctx.precreated) connector.warmUp()
    connector.whenTerminated().onComplete({
      case Success(_) => self ! Event.ConnectorStopped(id)
      case Failure(e) => self ! Event.ConnectorCrushed(id, e)
    })
    id -> connector
  }

  private def cancelJob(id: String, state: State, respond: ActorRef): State = {
    state.getWithState(id) match {
      case Some((ref, status)) =>
        ref.tell(JobActor.Event.Cancel, respond)
        status match {
          case Waiting => state.done(id)
          case Working => state
        }
      case None =>
        respond ! akka.actor.Status.Failure(new IllegalArgumentException(s"Unknown job: $id"))
        state
    }
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
    executorId: Option[String],
    failures: Int
  )
  object FrontendStatus {
    val empty: FrontendStatus = FrontendStatus(Map.empty, None, 0)
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
    connectorStarter: (String, ContextConfig) => WorkerConnector,
    jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsData], StatusReporter, JobLogger)],
    defaultInactiveTimeout: FiniteDuration
  ): Props = Props(classOf[ContextFrontend], name, status, loggersFactory, connectorStarter, jobFactory, defaultInactiveTimeout)


  def props(
    name: String,
    status: StatusReporter,
    loggersFactory: JobLoggersFactory,
    connectorStarter: (String, ContextConfig) => WorkerConnector
  ): Props = props(name, status, loggersFactory, connectorStarter, ActorF.props(JobActor.props _), 5 minutes)
}
