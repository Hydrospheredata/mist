package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.Messages.StatusMessages.FailedEvent
import io.hydrosphere.mist.master.execution.ContextFrontend.Event.JobDied
import io.hydrosphere.mist.master.execution.ContextFrontend.FrontendStatus
import io.hydrosphere.mist.master.execution.workers.{WorkerConnection, WorkerConnector}
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax}
import mist.api.data.JsLikeData

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

trait FrontendBasics {

  type State = FrontendState[String, ActorRef]
  val State = FrontendState

  def mkStatus(state: State, executorId: Option[String]): FrontendStatus = {
    val jobs =
      state.queued.map({case (k, _) => k -> ExecStatus.Queued}) ++
      state.active.map({case (k, _) => k -> ExecStatus.Started})

    FrontendStatus(
      jobs = jobs,
      executorId
    )
  }

  def mkStatus(state: State): FrontendStatus = mkStatus(state, None)
  def mkStatus(state: State, execId: String): FrontendStatus = mkStatus(state, Some(execId))

}

class ContextFrontend(
  name: String,
  reporter: StatusReporter,
  connectorStarter: (String, ContextConfig) => WorkerConnector,
  jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsLikeData], StatusReporter)]
) extends Actor
  with ActorLogging
  with ActorFSyntax
  with FrontendBasics {

  import ContextFrontend._

  import context.dispatcher

  override def receive: Receive = initial

  private def initial: Receive = {
    case ContextEvent.UpdateContext(ctx) =>
      nonConnectedCtxUpd(ctx, FrontendState.empty)(awaitRequest(ctx))
  }

  private def awaitRequest(ctx: ContextConfig): Receive = {
    case Event.Status => sender() ! FrontendStatus.empty
    case ContextEvent.UpdateContext(updCtx) =>
      nonConnectedCtxUpd(updCtx, FrontendState.empty)(awaitRequest(updCtx))

    case req: RunJobRequest =>
      val next = mkJob(req, FrontendState.empty, sender())
      val (id, connector) = startConnector(ctx)
      becomeWithConnector(ctx, next, ConnectorState.initial(id, connector))
  }

  // handle UpdateContext for awaitRequest/initial
  private def nonConnectedCtxUpd(ctx: ContextConfig, state: State)(stayReceive: => Receive): Unit = {
    if (ctx.precreated) {
      val (id, connector) = startConnector(ctx)
      becomeWithConnector(ctx, state, ConnectorState.initial(id, connector))
    } else {
      context become stayReceive
    }
  }

  // handle currentState changes, starting new jobs if it's possible
  private def becomeWithConnector(
    ctx: ContextConfig,
    state: State,
    connectorState: ConnectorState
  ): Unit = {

    def askConnection(): Unit = {
      connectorState.connector.askConnection().onComplete {
        case Success(connection) => self ! Event.Connection(connection)
        case Failure(e) => self ! Event.ConnectionFailure(e)
      }
    }

    val available = ctx.maxJobs - connectorState.all
    val need = math.min(state.queued.size - connectorState.asked, available)
    val nextConnState = {
      if (need > 0) {
        for (_ <- 0 until need) askConnection()
        connectorState.copy(asked = connectorState.asked + need)
      } else
        connectorState
    }

    context become withConnector(ctx, state, nextConnState)
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
      case Event.Status => sender() ! mkStatus(currentState, connectorState.id)
      case ContextEvent.UpdateContext(updCtx) =>
        val (id, connector) = startConnector(updCtx)
        becomeWithConnector(ctx, currentState, connectorState.copy(id = id, connector = connector))

      case req: RunJobRequest => becomeNextState(mkJob(req, currentState, sender()))
      case CancelJobRequest(id) => becomeNextState(cancelJob(id, currentState, sender()))

      case Event.Connection(worker) =>
        log.info("Received new connection!")
        currentState.nextOption match {
          case Some((id, ref)) =>
            ref ! JobActor.Event.Perform(worker)
            becomeNext(connectorState.askSuccess, currentState.toWorking(id))
          case None =>
            //TODO notify connection that it's unused
            //TODO exclusive workers leak
            log.warning("NOT IMpLEMETED")
        }

      case Event.ConnectionFailure(e) =>
        log.error(s"Ask new worker connection for $name failed")
        if (connectorState.failed > ConnectionFailedMaxTimes) {
          connectorState.connector.shutdown(true)
          context become sleepingTilUpdate(ctx, e)
        } else {
          becomeNextConn(connectorState.askFailure)
        }

      case JobActor.Event.Completed(id) if currentState.hasWorking(id) =>
        becomeNext(connectorState.connectionReleased, currentState.done(id))

      case JobActor.Event.Completed(id) =>
        log.warning(s"Received unexpected completed event from $id")

      case Event.ConnectorCrushed(id, e) =>
        log.error(e, "Executor {} died", id)
        val newState = connectorState.connectorFailed
        if (newState.connectorFailedTimes > ConnectorFailedMaxTimes) {
          connectorState.connector.shutdown(true)
          context become sleepingTilUpdate(ctx, e)
        } else {
          val (newId, newConn) = startConnector(ctx)
          becomeWithConnector(ctx, currentState, newState.copy(id = newId, connector = newConn))
        }

      case Event.ConnectorStopped(id) =>
        log.error(s"Executor $id died")
        val (newId, newConn) = startConnector(ctx)
        becomeWithConnector(ctx, currentState, connectorState.copy(id = newId, connector = newConn))
    }
  }

  private def sleepingTilUpdate(brokenCtx: ContextConfig, error: Throwable): Receive = {
    case Event.Status => mkStatus(FrontendState.empty) // TODO: return failed currentState
    case ContextEvent.UpdateContext(updCtx) =>
      nonConnectedCtxUpd(updCtx, FrontendState.empty)(sleepingTilUpdate(brokenCtx, error))
    case req: RunJobRequest =>
      respondWithError(brokenCtx, req, sender(), error)
  }

  private def startConnector(ctx: ContextConfig): (String, WorkerConnector) = {
    val id = UUID.randomUUID().toString
    log.info(s"Starting executor $id for $name")
    val connector = connectorStarter(id, ctx)
    if (ctx.precreated) connector.warmUp()
    connector.whenTerminated().onComplete({
      case Success(_) => self ! Event.ConnectorStopped(id)
      case Failure(e) => self ! Event.ConnectorCrushed(id, e)
    })
    id -> connector
  }

  private def cancelJob(id: String, state: State, respond: ActorRef): State = state.getWithState(id) match {
    case Some((ref, Working)) =>
      ref.tell(JobActor.Event.Cancel, respond)
      state
    case Some((ref, Waiting)) =>
      ref.tell(JobActor.Event.Cancel, respond)
      state.remove(id)
    case None =>
      respond ! akka.actor.Status.Failure(new IllegalArgumentException(s"Unknown job: $id"))
      state
  }

  private def mkJob(req: RunJobRequest, st: State, respond: ActorRef): State = {
    val promise = Promise[JsLikeData]
    val ref = jobFactory.create(self, req, promise, reporter)
    context.watchWith(ref, JobDied(req.id))

    respond ! ExecutionInfo(req, promise)
    st.enqueue(req.id, ref)
  }

  private def respondWithError(brokenCtx: ContextConfig, req: RunJobRequest, respond: ActorRef, error: Throwable): Unit = {
    val msg = buildErrorMessage(
      s"Please update context ${brokenCtx.name} before running function ${req.params.className} invocation",
      error
    )
    val promise = Promise[JsLikeData].failure(error)
    reporter.report(FailedEvent(req.id, System.currentTimeMillis(), msg))
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
  val ConnectionFailedMaxTimes = 50
  val ConnectorFailedMaxTimes = 50

  sealed trait Event
  object Event {
    final case class ConnectorCrushed(id: String, err: Throwable) extends Event
    final case class ConnectorStopped(id: String) extends Event

    final case class JobDied(id: String) extends Event
    final case class JobCompleted(id: String) extends Event

    final case class Connection(conn: WorkerConnection) extends Event
    final case class ConnectionFailure(e: Throwable) extends Event

    case object Status extends Event
  }

  case class FrontendStatus(
    jobs: Map[String, ExecStatus],
    executorId: Option[String]
  )
  object FrontendStatus {
    val empty: FrontendStatus = FrontendStatus(Map.empty, None)
  }

  case class ConnectorState(
    id: String,
    connector: WorkerConnector,
    used: Int,
    asked: Int,
    failed: Int,
    connectorFailedTimes: Int
  ) {

    def all: Int = used + asked
    def askSuccess: ConnectorState = copy(used = used + 1, asked = asked - 1, failed = 0, connectorFailedTimes = 0)
    def askFailure: ConnectorState = copy(asked = asked - 1, failed = failed + 1)
    def connectionReleased: ConnectorState = copy(used = used - 1)
    def connectorFailed: ConnectorState = copy(failed = 0, connectorFailedTimes = connectorFailedTimes + 1)
  }

  object ConnectorState {
    def initial(id: String, connector: WorkerConnector): ConnectorState =
      ConnectorState(id, connector, 0, 0, 0, 0)
  }

  def props(
    name: String,
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => WorkerConnector,
    jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsLikeData], StatusReporter)]
  ): Props = Props(classOf[ContextFrontend], name, status, executorStarter, jobFactory)


  def props(
    name: String,
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => WorkerConnector
  ): Props = props(name, status, executorStarter, ActorF.props(JobActor.props _))
}
