package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.Messages.WorkerMessages._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

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

//TODO: cancel jobs should send info to statusService
//TODO: if worker crashed - jobs taht us in ruunning status should be marked as Failure
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

  private def cancelRunningJob(sender: ActorRef, worker: ActorRef, id: String): Unit = {
    implicit val timeout = Timeout(5.seconds)
    val f = (worker ? CancelJobRequest(id)).mapTo[JobIsCancelled]
    f.onSuccess({
      case x @ JobIsCancelled(id, time) =>
        log.info("Job {} is cancelled", id)
        statusService ! CanceledEvent(id, time)
        sender ! x
    })
    f.onFailure({
      case e: Throwable =>
        log.error(e, "Job {} cancellation failed on backed", id)
    })
  }

  private def cancelQueuedJob(sender: ActorRef, id: String): Unit = {
    queue.dequeueFirst(_.request.id == id)
    jobs -= id
    sender ! JobIsCancelled(id)
    statusService ! CanceledEvent(id, System.currentTimeMillis())
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




