package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.execution.ContextFrontend.Event.JobDied
import io.hydrosphere.mist.master.execution.ContextFrontend.FrontendStatus
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
  executorStarter: (String, ContextConfig) => Future[ActorRef],
  jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsLikeData], StatusReporter)]
) extends Actor
  with ActorLogging
  with ActorFSyntax
  with FrontendBasics {

  import ContextFrontend._

  override def receive: Receive = initial

  private def initial: Receive = {
    case Event.UpdateContext(ctx) => context become awaitRequest(ctx)
  }

  private def awaitRequest(ctx: ContextConfig): Receive = {
    case Event.Status => sender() ! FrontendStatus.empty
    case Event.UpdateContext(updCtx) => context become awaitRequest(updCtx)

    case req: RunJobRequest =>
      val next = mkJob(req, FrontendState.empty, sender())
      val id = startExecutor(ctx)
      context become awaitExecutor(ctx, next, id)
  }

  private def awaitExecutor(ctx: ContextConfig, state: State, execId: String): Receive = {
    def becomeNext(st: State): Unit = context become awaitExecutor(ctx, st, execId)

    {
      case Event.Status => sender() ! mkStatus(state)
      case Event.UpdateContext(updCtx) => log.warning("NON IMPLEMENTED")

      case req: RunJobRequest => becomeNext(mkJob(req, state, sender()))
      case CancelJobRequest(id) => becomeNext(cancelJob(id, state, sender()))

      case Event.ExecutorStartSuccess(id, ref) if id == execId =>
        log.info(s"Executor for $name started: $id")
        context.watchWith(ref, Event.ExecutorDied(id, ref))
        becomeWithExecutor(ctx, state, ref)

      case Event.ExecutorStartFailure(id, err) if id == execId =>
        log.error(err, s"Executor $id startup failed")

      // handle Started/Completed in case if we updated executor
      // and there are jobs that started its execution on previous executor
      case JobActor.Event.Started(id) if state.hasGranted(id) => becomeNext(state.toWorking(id))
      case JobActor.Event.Started(id) =>
        log.warning(s"Received unexpected started event from $id")

      case JobActor.Event.Completed(id) if state.hasWorking(id) => becomeNext(state.done(id))
      case JobActor.Event.Completed(id) =>
        log.warning(s"Received unexpected completed event from $id")
    }
  }

  // handle state changes, starting new jobs if it's possible
  private def becomeWithExecutor(ctx: ContextConfig, state: State, executor: ActorRef): Unit = {
    val next = state.grantNext(ctx.maxJobs - state.inProgressSize)((id, ref) => {
      log.info(s"Trying to start job $id")
      ref ! JobActor.Event.Perform(executor)
    })
    context become withExecutor(ctx, next, executor)
  }

  private def withExecutor(ctx: ContextConfig, state: State, executor: ActorRef): Receive = {
    def becomeNext(state: State): Unit = becomeWithExecutor(ctx, state, executor)

    {
      case Event.Status => sender() ! mkStatus(state)
      case Event.UpdateContext(updCtx) => log.warning("NON IMPLEMENTED")

      case req: RunJobRequest => becomeNext(mkJob(req, state, sender()))
      case CancelJobRequest(id) => becomeNext(cancelJob(id, state, sender()))

      case JobActor.Event.Started(id) if state.hasGranted(id) => becomeNext(state.toWorking(id))
      case JobActor.Event.Started(id) =>
        log.warning(s"Received unexpected started event from $id")

      case JobActor.Event.Completed(id) if state.hasWorking(id) => becomeNext(state.done(id))
      case JobActor.Event.Completed(id) =>
        log.warning(s"Received unexpected completed event from $id")

      //TODO? restart timeouts
      case Event.ExecutorDied(id, ref) if ref == executor =>
        log.error(s"Executor $id died")
        val newId = startExecutor(ctx)
        context become awaitExecutor(ctx, state, newId)
    }
  }


  private def startExecutor(ctx: ContextConfig): String = {
    implicit val ec = context.system.dispatcher

    val id = UUID.randomUUID().toString
    log.info(s"Starting executor $id for $name")
    executorStarter(id, ctx).onComplete {
      case Success(ref) => self ! Event.ExecutorStartSuccess(id, ref)
      case Failure(err) => self ! Event.ExecutorStartFailure(id, err)
    }
    id
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
    final case class ExecutorStartSuccess(id: String, ref: ActorRef) extends Event
    final case class ExecutorStartFailure(id: String, err: Throwable) extends Event
    final case class ExecutorDied(id: String, ref: ActorRef) extends Event

    final case class JobDied(id: String) extends Event
    final case class JobCompleted(id: String) extends Event

    case object Status extends Event
  }

  case class FrontendStatus(
    jobs: Map[String, ExecStatus],
    executorId: Option[String]
  )
  object FrontendStatus {
    val empty: FrontendStatus = FrontendStatus(Map.empty, None)
  }

  def props(
    name: String,
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => Future[ActorRef],
    jobFactory: ActorF[(ActorRef, RunJobRequest, Promise[JsLikeData], StatusReporter)]
  ): Props = Props(classOf[ContextFrontend], name, status, executorStarter, jobFactory)


  def props(
    name: String,
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => Future[ActorRef]
  ): Props = props(name, status, executorStarter, ActorF.props(JobActor.props _))
}
