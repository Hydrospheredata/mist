package io.hydrosphere.mist.master.namespace

import java.util.UUID

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.jobs.{JobDetails, JobExecutionParams, JobResult}
import io.hydrosphere.mist.master.http.JobExecutionStatus
import io.hydrosphere.mist.master.namespace.JobMessages.RunJobRequest
import io.hydrosphere.mist.master.namespace.NamespaceJobExecutor.GetActiveJobs
import io.hydrosphere.mist.utils.Logger

import JobMessages._
import WorkerMessages._

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class Namespace(
  name: String,
  workersManager: ActorRef
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global

  def startJob(execParams: JobExecutionParams): Future[JobResult] = {
    val request = RunJobRequest(
      id = UUID.randomUUID().toString,
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
  promise: Promise[Map[String, Any]],
  var status: JobDetails.Status
) {

  def updateStatus(s: JobDetails.Status): ExecutionInfo = {
    this.status = s
    this
  }
}

object ExecutionInfo {

  def apply(req: RunJobRequest, status: JobDetails.Status): ExecutionInfo =
    ExecutionInfo(req, Promise[Map[String, Any]], status)

}

class NamespaceJobExecutor(
  name: String,
  maxRunningJobs: Int
) extends Actor with ActorLogging {

  val queue = mutable.Queue[ExecutionInfo]()
  val jobs = mutable.HashMap[String, ExecutionInfo]()

  var counter = 0

  override def receive: Actor.Receive = noWorker

  val common: Receive = {
    case GetActiveJobs =>
      sender() ! jobs.values
        .map(i => JobExecutionStatus(i.request.id))
  }

  private def noWorker: Receive = common orElse {
    case r: RunJobRequest =>
      val info = queueRequest(r)
      sender() ! info

    case WorkerUp(worker) =>
      sendQueued(worker)
      context become withWorker(worker)

    case CancelJobRequest(id) =>
      jobs.get(id).foreach(info => {
        queue.dequeueFirst(_.request.id == id)
        jobs -= id
        sender() ! JobIsCancelled(id)
      })
  }

  private def withWorker(worker: ActorSelection): Receive = common orElse {
    case r: RunJobRequest =>
      val info = queueRequest(r)
      if (counter < maxRunningJobs) {
        sendJob(worker, info)
        counter = counter + 1
      }
      sender() ! info

    case JobStarted(id, _) =>
      jobs.get(id).foreach(info => {
        log.info(s"Job has been started $id")
        info.updateStatus(JobDetails.Status.Running)
      })

    case done: JobResponse =>
      onJobDone(done)
      sendQueued(worker)

    case WorkerDown =>
      log.info(s"Worker is down $name")
      context become noWorker

    case req @ CancelJobRequest(id) =>
      jobs.get(id).foreach(i => {
        val originSender = sender()
        if (i.status == JobDetails.Status.Queued) {
          cancelQueuedJob(originSender, id)
        } else if (i.status == JobDetails.Status.Running) {
          cancelRunningJob(originSender, worker, id)
        }
      })
  }

  private def cancelRunningJob(sender: ActorRef, worker: ActorSelection, id: String): Unit = {
    implicit val timeout = Timeout(5.seconds)
    val f = (worker ? CancelJobRequest(id)).mapTo[JobIsCancelled]
    f.onSuccess({
      case x @ JobIsCancelled(id, time) =>
        //jobs -= id
        sender ! x
    })
  }

  private def cancelQueuedJob(sender: ActorRef, id: String): Unit = {
    queue.dequeueFirst(_.request.id == id)
    jobs -= id
    sender ! JobIsCancelled(id)
  }

  private def onJobDone(resp: JobResponse): Unit = {
    jobs.remove(resp.id).foreach(info => {
      log.info(s"Job ${info.request} id done with result $resp")
      counter = counter -1
      resp match {
        case JobFailure(_, error) =>
          info.promise.failure(new RuntimeException(error))
        case JobSuccess(_, r) =>
          info.promise.success(r)
      }
    })
  }

  private def queueRequest(req: RunJobRequest): ExecutionInfo = {
    val info = ExecutionInfo(req, JobDetails.Status.Queued)
    queue += info
    jobs += req.id -> info
    info
  }

  private def sendQueued(worker: ActorSelection): Unit = {
    val max = maxRunningJobs - counter
    for {
      _ <- 1 to max
      if queue.nonEmpty
    } yield {
      val info = queue.dequeue()
      sendJob(worker, info)
      counter = counter + 1
    }
  }

  private def sendJob(worker: ActorSelection, info: ExecutionInfo): Unit = {
    worker ! info.request
  }

}

object NamespaceJobExecutor {

  def props(name: String, maxRunningJobs: Int): Props =
    Props(classOf[NamespaceJobExecutor], name, maxRunningJobs)


  case object GetActiveJobs
}




