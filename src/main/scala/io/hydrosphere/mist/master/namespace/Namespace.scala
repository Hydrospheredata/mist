package io.hydrosphere.mist.master.namespace

import java.util.UUID

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.jobs.{JobExecutionParams, JobResult}
import io.hydrosphere.mist.master.namespace.WorkerActor._
import io.hydrosphere.mist.master.namespace.WorkersManager.{WorkerCommand, WorkerDown, WorkerUp}
import io.hydrosphere.mist.utils.Logger

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Namespace(
  name: String,
  workersManager: ActorRef
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global

  def startJob(execParams: JobExecutionParams): Future[JobResult] = {
    val request = RunJobRequest(
      id = s"$name-${execParams.className}-${UUID.randomUUID().toString}",
      JobParams(
        filePath = execParams.path,
        className = execParams.className,
        arguments = execParams.parameters,
        action = execParams.action
      )
    )

    val promise = Promise[JobResult]
    // that timeout only for obtaining execution info
    implicit val timeout = Timeout(30.seconds)

    workersManager.ask(WorkerCommand(name, request))
      .mapTo[ExecutionInfo]
      .flatMap(_.promise.future).onComplete({
        case Success(r) =>
          promise.success(JobResult.success(r, execParams))
        case Failure(e) =>
          promise.success(JobResult.failure(e.getMessage, execParams))
    })

    promise.future
  }

}

case class ExecutionInfo(
  request: RunJobRequest,
  promise: Promise[Map[String, Any]]
)

object ExecutionInfo {

  def apply(req: RunJobRequest): ExecutionInfo =
    ExecutionInfo(req, Promise[Map[String, Any]])

}

class NamespaceJobExecutor(
  name: String,
  maxRunningJobs: Int
) extends Actor with ActorLogging {

  var queue = mutable.Queue[ExecutionInfo]()
  var running = mutable.HashMap[String, ExecutionInfo]()

  override def receive: Actor.Receive = noWorker

  private def noWorker: Receive = {
    case r: RunJobRequest =>
      val info = ExecutionInfo(r)
      queue += info
      sender() ! info

    case WorkerUp(worker) =>
      sendQueued(worker)
      context become withWorker(worker)

  }

  private def withWorker(w: ActorSelection): Receive = {
    case r: RunJobRequest =>
      val info = ExecutionInfo(r)
      if (running.size < maxRunningJobs) {
        sendJob(w, info)
      } else {
        queue += info
      }
      sender() ! info

    case started: JobStarted =>
      log.info(s"Job has been started ${started.id}")

    case success: JobSuccess =>
      running.get(success.id) match {
        case Some(i) =>
          i.promise.success(success.result)
          running -= success.id
        case None =>
          log.warning(s"WTF? $success")
      }
      log.info(s"Job ${success.id} ended")
      sendQueued(w)

    case failure: JobFailure =>
      running.get(failure.id) match {
        case Some(i) =>
          i.promise.failure(new RuntimeException(failure.error))
          running -= failure.id
        case None =>
          log.warning(s"WTF? $failure")
      }
      sendQueued(w)
      log.info(s"Job ${failure.id} is failed")

    case WorkerDown =>
      log.info(s"Worker is down $name")
      context become noWorker
  }

  private def sendQueued(worker: ActorSelection): Unit = {
    val max = maxRunningJobs - running.size
    for {
      _ <- 1 to max
      if queue.nonEmpty
    } yield {
      val info = queue.dequeue()
      sendJob(worker, info)
    }
  }

  private def sendJob(to: ActorSelection, info: ExecutionInfo): Unit = {
    to ! info.request
    running += info.request.id -> info
  }

}

object NamespaceJobExecutor {

  def props(name: String, maxRunningJobs: Int): Props =
    Props(classOf[NamespaceJobExecutor], name, maxRunningJobs)
}




