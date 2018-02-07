package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
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

  implicit val ec = context.system.dispatcher

  override def receive: Receive = initial

  private def initial: Receive = {
    case Event.UpdateContext(ctx) => context become awaitRequest(ctx)
  }

  private def awaitRequest(ctx: ContextConfig): Receive = {
    case Event.Status => sender() ! FrontendStatus.empty
    case Event.UpdateContext(updCtx) =>
      sender() ! akka.actor.Status.Success(())
      context become awaitRequest(updCtx)

    case req: RunJobRequest =>
      val next = mkJob(req, FrontendState.empty, sender())
      val (id, connector) = startConnector(ctx)
      connector.whenTerminated().onComplete({
        case Success(_) => self ! Event.ConnectorStopped(id)
        case Failure(e) => self ! Event.ConnectorCrushed(id, e)
      })
      becomeWithConnector(ctx, next, UsedConnections.empty, connector)
  }


  // handle state changes, starting new jobs if it's possible
  private def becomeWithConnector(
    ctx: ContextConfig,
    state: State,
    usedConnections: UsedConnections,
    connector: WorkerConnector
  ): Unit = {
    def askConnection(): Unit = {
      connector.askConnection().onComplete {
        case Success(connection) => self ! Event.Connection(connection)
        case Failure(e) => self ! Event.ConnectionFailure(e)
      }
    }

    val available = ctx.maxJobs - usedConnections.all
    val need = math.min(state.queued.size - usedConnections.asked, available)
    val nextConn = {
      if (need > 0) {
        for (_ <- 0 until need) askConnection()
        usedConnections.copy(asked = usedConnections.asked + need)
      } else
        usedConnections
    }

    context become withConnector(ctx, state, nextConn, connector)
  }

  private def withConnector(
    ctx: ContextConfig,
    state: State,
    conns: UsedConnections,
    connector: WorkerConnector): Receive = {

    def becomeNextState(state: State): Unit = becomeWithConnector(ctx, state, conns, connector)
    def becomeNextConn(conns: UsedConnections): Unit = becomeWithConnector(ctx, state, conns, connector)
    def becomeNext(c: UsedConnections, s: State): Unit = becomeWithConnector(ctx, s, c, connector)

    {
      case Event.Status => sender() ! mkStatus(state)
      case Event.UpdateContext(updCtx) => log.warning("NON IMPLEMENTED")

      case req: RunJobRequest => becomeNextState(mkJob(req, state, sender()))
      case CancelJobRequest(id) => becomeNextState(cancelJob(id, state, sender()))

      case Event.Connection(worker) =>
        log.info("Received new connection!")
        state.nextOption match {
          case Some((id, ref)) =>
            ref ! JobActor.Event.Perform(worker)
            becomeNext(conns.askSuccess, state.toWorking(id))
          case None =>
            //TODO notify connection that it's unused
            //TODO exclusive workers leak
            log.warning("NOT IMpLEMETED")
        }

      case Event.ConnectionFailure(e) =>
        log.error(s"Ask new worker connection for $name failed")
        becomeNextConn(conns.askFailure)

      case JobActor.Event.Completed(id) if state.hasWorking(id) => becomeNext(conns.connectionReleased, state.done(id))
      case JobActor.Event.Completed(id) =>
        log.warning(s"Received unexpected completed event from $id")

      //TODO? restart timeouts
      case Event.ConnectorCrushed(id, ref) if ref == connector =>
        log.error(s"Executor $id died")
        val (newId, newConn) = startConnector(ctx)
        becomeWithConnector(ctx, state, UsedConnections.empty, newConn)
    }
  }


  private def startConnector(ctx: ContextConfig): (String, WorkerConnector) = {
    val id = UUID.randomUUID().toString
    log.info(s"Starting executor $id for $name")
    id -> connectorStarter(id, ctx)
  }

  private def cancelJob(id: String, state: State, respond: ActorRef): State = state.get(id) match {
    case Some(ref) =>
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

}

object ContextFrontend {

  sealed trait Event
  object Event {
    final case class UpdateContext(context: ContextConfig) extends Event

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

  case class UsedConnections(used: Int, asked: Int) {
    def all:Int = used + asked
    def askSuccess: UsedConnections = copy(used = used + 1, asked - 1)
    def askFailure: UsedConnections = copy(asked = asked - 1)
    def connectionReleased: UsedConnections = copy(used = used - 1)
  }

  object UsedConnections {
    val empty: UsedConnections = UsedConnections(0, 0)
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
