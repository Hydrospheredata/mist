package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.jobs.{JobResult, JobDetails}
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.master.models.JobStartResponse

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._

//TODO don't use public status struct
case class ExecutionInfo(
  request: RunJobRequest,
  promise: Promise[Map[String, Any]],
  var status: JobDetails.Status
) {

  def updateStatus(s: JobDetails.Status): ExecutionInfo = {
    this.status = s
    this
  }

  def toJobStartResponse: JobStartResponse = JobStartResponse(request.id)

  def toJobResult: Future[JobResult] = {
    val result = Promise[JobResult]
    promise.future.onComplete {
      case Success(r) =>
        result.success(JobResult.success(r))
      case Failure(e) =>
        result.success(JobResult.failure(e.getMessage))
    }
    result.future
  }
}

object ExecutionInfo {

  def apply(req: RunJobRequest, status: JobDetails.Status): ExecutionInfo =
    ExecutionInfo(req, Promise[Map[String, Any]], status)

  def apply(req: RunJobRequest): ExecutionInfo =
    ExecutionInfo(req, Promise[Map[String, Any]], JobDetails.Status.Queued)
}

//TODO: if worker crashed - jobs that is in running status should be marked as Failure
/**
  * Queue for jobs before sending them to worker
  */
class FrontendJobExecutor(
  name: String,
  maxRunningJobs: Int,
  statusService: ActorRef
) extends Actor with ActorLogging {

  val queue = mutable.Queue[ExecutionInfo]()
  val jobs = mutable.HashMap[String, ExecutionInfo]()

  var counter = 0

  override def receive: Actor.Receive = noWorker

  val common: Receive = {
    case GetActiveJobs =>
      sender() ! jobs.values.map(i => JobExecutionStatus(i.request.id, name, status = i.status))
    case FailRemainingJobs(reason) =>
      jobs.keySet
        .map(JobFailure(_, reason))
        .foreach(onJobDone)
  }

  private def noWorker: Receive = common orElse {
    case r: RunJobRequest =>
      val info = queueRequest(r)
      sender() ! info

    case WorkerUp(worker) =>
      sendQueued(worker)
      context become withWorker(worker)

    case CancelJobRequest(id) =>
      cancelQueuedJob(sender(), id)
  }

  private def withWorker(worker: ActorRef): Receive = common orElse {
    case r: RunJobRequest =>
      val info = queueRequest(r)
      if (counter < maxRunningJobs) {
        sendQueued(worker)
      }
      sender() ! info

    case JobStarted(id, time) =>
      jobs.get(id).foreach(info => {
        log.info(s"Job has been started $id")
        statusService ! StartedEvent(id, time)
        info.updateStatus(JobDetails.Status.Started)
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
        } else if (i.status == JobDetails.Status.Started) {
          cancelRunningJob(originSender, worker, id)
        }
      })
  }

  private def cancelRunningJob(sender: ActorRef, worker: ActorRef, id: String): Unit = {
    implicit val timeout = Timeout(5.seconds)
    val f = (worker ? CancelJobRequest(id)).mapTo[JobIsCancelled]
    f.onSuccess({
      case x @ JobIsCancelled(id, time) =>
        log.info("Job {} is cancelled", id)
        val event = CanceledEvent(id, time)
        sendStatusUpdate(event).foreach(_ => {
          sender ! JobIsCancelled(id)
        })
    })
    f.onFailure({
      case e: Throwable =>
        log.error(e, "Job {} cancellation failed on backed", id)
    })
  }

  private def cancelQueuedJob(sender: ActorRef, id: String): Unit = {
    queue.dequeueFirst(_.request.id == id)
    jobs -= id
    val event = CanceledEvent(id, System.currentTimeMillis())
    sendStatusUpdate(event).foreach(_ => sender ! JobIsCancelled(id))
  }

  private def onJobDone(resp: JobResponse): Unit = {
    jobs.remove(resp.id).foreach(info => {
      log.info(s"Job ${info.request} id done with result $resp")
      counter = counter -1
      val statusEvent = resp match {
        case JobFailure(id, error) =>
          info.promise.failure(new RuntimeException(error))
          FailedEvent(id, System.currentTimeMillis(), error)
        case JobSuccess(id, r) =>
          info.promise.success(r)
          FinishedEvent(id, System.currentTimeMillis(), r)
      }
      statusService ! statusEvent
    })
  }

  private def sendStatusUpdate(e: UpdateStatusEvent): Future[Unit] =
    statusService.ask(e)(Timeout(5.second)).map(_ => ())

  private def queueRequest(req: RunJobRequest): ExecutionInfo = {
    val info = ExecutionInfo(req, JobDetails.Status.Queued)
    queue += info
    jobs += req.id -> info

    statusService ! QueuedEvent(req.id)

    info
  }

  private def sendQueued(worker: ActorRef): Unit = {
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

  private def sendJob(worker: ActorRef, info: ExecutionInfo): Unit = {
    worker ! info.request
  }

}

object FrontendJobExecutor {

  def props(
    name: String,
    maxRunningJobs: Int,
    statusService: ActorRef
  ): Props =
    Props(classOf[FrontendJobExecutor], name, maxRunningJobs, statusService)


}




