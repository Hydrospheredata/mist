package io.hydrosphere.mist.worker

import java.util.concurrent.{Executors, ExecutorService}

import akka.actor._
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.worker.runners.{MistJobRunner, JobRunner}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

case class ExecutionUnit(
  requester: ActorRef,
  jobFuture: Future[Either[String, Map[String, Any]]]
)

trait JobStarting { that: Actor =>

  val namedContext: NamedContext
  val runner: JobRunner

  def startJob(req: RunJobRequest)(implicit ec: ExecutionContext): Future[Either[String, Map[String, Any]]] = {
    val id = req.id
    val future = Future {
      namedContext.context.setJobGroup(req.id, req.id)
      runner.run(req.params, namedContext)
    }

    future.onComplete(r => {
      val message = r match {
        case Success(Left(error)) => JobFailure(id, error)
        case Success(Right(value)) => JobSuccess(id, value)
        case Failure(e) => JobFailure(id, e.getMessage)
      }
      self ! message
    })

    future
  }
}

class SharedWorkerActor(
  val namedContext: NamedContext,
  val runner: JobRunner,
  idleTimeout: Duration,
  maxJobs: Int
) extends Actor with JobStarting with ActorLogging {

  val activeJobs = mutable.Map[String, ExecutionUnit]()

  implicit val ec = {
    val service = Executors.newFixedThreadPool(maxJobs)
    ExecutionContext.fromExecutorService(
      service,
      e => log.error(e, "Error from thread pool")
    )
  }

  override def preStart(): Unit = {
    context.setReceiveTimeout(idleTimeout)
  }

  override def receive: Receive = {
    case req @ RunJobRequest(id, params) =>
      if (activeJobs.size == maxJobs) {
        sender() ! WorkerIsBusy(id)
      } else {
        val future = startJob(req)
        log.info(s"Starting job: $id")
        activeJobs += id -> ExecutionUnit(sender(), future)
        sender() ! JobStarted(id)
      }

    // it does not work for streaming jobs
    case CancelJobRequest(id) =>
      activeJobs.get(id) match {
        case Some(u) =>
          namedContext.context.cancelJobGroup(id)
          sender() ! JobIsCancelled(id)
        case None =>
          log.warning(s"Can not cancel unknown job $id")
      }

    case x: JobResponse =>
      log.info(s"Jon execution done. Result $x")
      activeJobs.get(x.id) match {
        case Some(unit) =>
          unit.requester forward x
          activeJobs -= x.id

        case None =>
          log.warning(s"Corrupted worker state, unexpected receiving {}", x)
      }

    case ReceiveTimeout if activeJobs.isEmpty =>
      log.info(s"There is no activity on worker: $namedContext.. Stopping")
      context.stop(self)
  }

  override def postStop(): Unit = {
    ec.shutdown()
  }

}

class ExclusiveWorker(
  val namedContext: NamedContext,
  val runner: JobRunner
) extends Actor with JobStarting with ActorLogging {

  implicit val ec = {
    val service = Executors.newSingleThreadExecutor()
    ExecutionContext.fromExecutorService(service)
  }

  override def receive: Receive = awaitRequest

  val awaitRequest: Receive = {
    case req @ RunJobRequest(id, params) =>
      val future = startJob(req)
      sender() ! JobStarted(id)
      val unit = ExecutionUnit(sender(), future)
      context.become(execute(unit))
      log.info(s"Starting job: $id")
  }

  def execute(executionUnit: ExecutionUnit): Receive = {
    case CancelJobRequest(id) =>
      namedContext.context.cancelJobGroup(id)
      sender() ! JobIsCancelled(id)
      context.stop(self)

    case x: JobResponse =>
      log.info(s"Jon execution done. Result $x")
      executionUnit.requester forward x
      context.stop(self)
  }

  override def postStop(): Unit = {
    ec.shutdown()
  }

}

object WorkerActor {

  def props(mode: WorkerMode, context: NamedContext): Props = props(mode, context, MistJobRunner)

  def props(
    mode: WorkerMode,
    context: NamedContext,
    runner: JobRunner): Props = {

    mode match {
      case Shared(maxJobs, idleTimeout) =>
        Props(classOf[SharedWorkerActor], context, runner, idleTimeout, maxJobs)
      case Exclusive =>
        Props(classOf[ExclusiveWorker], context, runner)
    }
  }


}
