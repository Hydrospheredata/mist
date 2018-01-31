package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest, WorkerInitInfo}
import io.hydrosphere.mist.master.JobDetails.InProgress
import io.hydrosphere.mist.master.execution.ContextFrontend.Event.JobDied
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.master.{ExecutionInfo, JobDetails}
import mist.api.data.JsLikeData

import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class JobsState(actors: Map[String, ActorRef], queue: Queue[String]) {

  def enqueue(id: String, ref: ActorRef): JobsState = JobsState(actors + (id -> ref), queue.enqueue(id))

  def head: Option[ActorRef] = headId.flatMap(id => actors.get(id))

  def headId: Option[String] = if (queue.isEmpty) None else Some(queue.front)

  def get(id: String): Option[ActorRef] = actors.get(id)

  def remove(id: String): JobsState = JobsState(actors - id, queue.filter(_ != id))

  def removeFromQueue(id: String): JobsState = JobsState(actors, queue.filter(_ != id))

  def dequeue(id: String): JobsState = JobsState(actors, queue.filter(_ != id))

  def hasQueued(id: String): Boolean = queue.contains(id)

  def has(id: String): Boolean = actors.keys.exists(_ == id)

  def take(n: Int): Map[String, ActorRef] = {
    val min = math.min(n, queue.size)
    queue.take(min).map(i => i -> actors(i)).toMap
  }
}

object JobsState {

  val empty: JobsState = JobsState(Map.empty[String, ActorRef], Queue.empty)

  def apply(actors: Map[String, ActorRef], queue: Queue[String]): JobsState = new JobsState(actors, queue)

  def apply(id: String, ref: ActorRef): JobsState = empty.enqueue(id, ref)
}


class ContextFrontend(
  name: String,
  status: ActorRef,
  executorStarter: (String, ContextConfig) => Future[ActorRef],
  jobFactory: (ActorRef, RunJobRequest, Promise[JsLikeData], ActorRef) => Props
) extends Actor with ActorLogging {

  import ContextFrontend._

  override def receive: Receive = initial

  private def initial: Receive = {
    case Event.UpdateContext(ctx) => context become awaitRequest(ctx)
  }

  private def awaitRequest(ctx: ContextConfig): Receive = {
    case Event.UpdateContext(updCtx) => context become awaitRequest(updCtx)

    case req: RunJobRequest =>
      val next = mkJob(req, JobsState.empty, sender())
      val id = startExecutor(ctx)
      context become awaitExecutor(ctx, next, id)
  }

  private def awaitExecutor(
    ctx: ContextConfig,
    state: JobsState,
    execId: String
  ): Receive = {

    case req: RunJobRequest =>
      val next = mkJob(req, JobsState.empty, sender())
      context become awaitExecutor(ctx, next, execId)

    case CancelJobRequest(id) =>
      val next = cancelJob(id, state, sender())
      context become awaitExecutor(ctx, next, execId)

    case Event.ExecutorStartSuccess(id, ref) if id == execId =>
      log.info(s"Executor for $name started: $id")
      context.watchWith(ref, Event.ExecutorDied(id, ref))
      becomeWithExecutor(ctx, state, ref, Seq.empty)

    case Event.ExecutorStartFailure(id, err) if id == execId =>
      log.error(err, s"Executor ${id} startup failed")

    case JobActor.Event.Completed(id) =>
      if (state.has(id)) {
        val next = state.remove(id)
        log.info(s"Received completed event from $id")
        context become awaitExecutor(ctx, next, execId)
      } else {
        log.warning(s"Received unexpected completed event from $id")
      }
  }

  private def becomeWithExecutor(
    ctx: ContextConfig,
    state: JobsState,
    executor: ActorRef,
    inProgress: Seq[String]
  ): Unit = {
    val available = ctx.maxJobs - inProgress.size
    log.info(s"WTF? $available")
    if (available > 0) {
      state.take(available).foreach({case (id, ref) =>
        log.info(s"Trying to start job $id")
        ref ! JobActor.Event.Perform(executor)
      })
    }
    context become withExecutor(ctx, state, executor, inProgress)
  }

  private def withExecutor(
    ctx: ContextConfig,
    state: JobsState,
    executor: ActorRef,
    inProgress: Seq[String]
  ): Receive = {

    def becomeNext(state: JobsState, inProgress: Seq[String]): Unit =
      becomeWithExecutor(ctx, state, executor, inProgress)

    def updateState(state: JobsState): Unit = becomeNext(state, inProgress)

    def handleStarted(id: String): Unit = becomeNext(state.removeFromQueue(id), inProgress :+ id)

    def handleCompleted(id: String): Unit = becomeNext(state.remove(id), inProgress.filter(_ != id))

    {
      case req: RunJobRequest =>
        val next = mkJob(req, JobsState.empty, sender())
        updateState(next)

      case CancelJobRequest(id) =>
        val next = cancelJob(id, state, sender())
        updateState(next)

      case JobActor.Event.Started(id) =>
        if (state.hasQueued(id)) {
          handleStarted(id)
        } else {
          log.warning(s"Received unexpected started event from $id")
        }

      case JobActor.Event.Completed(id) =>
        if (state.has(id)) {
          handleCompleted(id)
        } else {
          log.warning(s"Received unexpected completed event from $id")
        }

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

  private def cancelJob(
    id: String,
    state: JobsState,
    respond: ActorRef
  ): JobsState = state.get(id) match {
    case Some(ref) =>
      ref.tell(JobActor.Event.Cancel, respond)
      state.dequeue(id)
    case None =>
      respond ! akka.actor.Status.Failure(new IllegalArgumentException(s"Unknown job: $id"))
      state
  }

  private def mkJob(req: RunJobRequest, st: JobsState, respond: ActorRef): JobsState = {
    val promise = Promise[JsLikeData]
    val props = jobFactory(self, req, promise, status)
    val ref = context.actorOf(props)
    context.watchWith(ref, JobDied(req.id))

    respond ! ExecutionInfo(req, promise, JobDetails.Status.Queued)
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
  }

  def props(
    name: String,
    status: ActorRef,
    executorStarter: (String, ContextConfig) => Future[ActorRef],
    jobFactory: (ActorRef, RunJobRequest, Promise[JsLikeData], ActorRef) => Props
  ): Props = Props(classOf[ContextFrontend], name, status, executorStarter, jobFactory)


  def props(
    name: String,
    status: ActorRef,
    executorStarter: (String, ContextConfig) => Future[ActorRef]
  ): Props = props(name, status, executorStarter, JobActor.props)
}
