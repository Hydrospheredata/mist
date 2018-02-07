package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, JobParams, RunJobRequest, WorkerInitInfo}
import io.hydrosphere.mist.master.Messages.JobExecution._
import io.hydrosphere.mist.master.execution.ExecutionInfo
import io.hydrosphere.mist.master.execution.workers.WorkersMirror
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.store.JobRepository

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  *  Jobs starting/stopping, statuses, worker utility methods
  */
class JobService(
  val execution: ActorRef,
  workersMirror: WorkersMirror,
  repo: JobRepository
) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.second)

  def activeJobs(): Future[Seq[JobDetails]] = repo.running()

  def markJobFailed(id: String, reason: String): Future[Unit] = {
    repo.path(id)(d => d.withEndTime(System.currentTimeMillis()).withFailure(reason))
  }

  def jobStatusById(id: String): Future[Option[JobDetails]] = repo.get(id)

  def endpointHistory(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] =
    repo.getByEndpointId(id, limit, offset, statuses)

  def getHistory(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] =
    repo.getAll(limit, offset, statuses)

  def workers(): Seq[WorkerLink] = workersMirror.workers()

  def workerByJobId(jobId: String): Future[Option[WorkerLink]] = {
    val res = for {
      job        <- OptionT(jobStatusById(jobId))
      workerId   <- OptionT.fromOption[Future](job.workerId)
      workerLink <- OptionT.fromOption[Future](workersMirror.worker(workerId))
    } yield workerLink
    res.value
  }

  def getWorkerInfo(workerId: String): Future[Option[WorkerFullInfo]] = {
    workersMirror.worker(workerId) match {
      case Some(link) =>
        // TODO
        repo.getByWorkerId(workerId).map(jobs => Some(WorkerFullInfo(
          name = link.name,
          address = link.address,
          sparkUi = link.sparkUi,
          jobs = jobs.map(toJobLinks),
          initInfo = WorkerInitInfo(
            sparkConf = Map.empty,
            maxJobs = 0,
            downtime = 1 hour,
            streamingDuration = 1 minute,
            logService = "",
            masterHttpConf = "",
            jobsSavePath = ""
          ))
        ))
      case None => Future.successful(None)
    }
  }

  private def toJobLinks(job: JobDetails): JobDetailsLink = JobDetailsLink(
      job.jobId, job.source, job.startTime, job.endTime,
      job.status, job.endpoint, job.workerId, job.createTime
  )

  // TODO
  def stopAllWorkers(): Future[Unit] = execution.ask(StopAllWorkers).map(_ => ())
  // TODO
  def stopWorker(workerId: String): Future[Unit] = execution.ask(StopWorker(workerId)).map(_ => ())

  def startJob(req: JobStartRequest): Future[ExecutionInfo] = {
    import req._

    val internalRequest = RunJobRequest(
      id = id,
      JobParams(
        filePath = endpoint.path,
        className = endpoint.className,
        arguments = parameters,
        action = action
      )
    )

    val startCmd = RunJobCommand(context, runMode, internalRequest)

    val details = JobDetails(
      endpoint.name,
      req.id,
      internalRequest.params,
      context.name,
      externalId, source)
    for {
      _ <- repo.update(details)
      info <- execution.ask(startCmd).mapTo[ExecutionInfo]
    } yield info
  }

  def stopJob(jobId: String): Future[Option[JobDetails]] = {
    import cats.data._
    import cats.implicits._

    def tryCancel(d: JobDetails): Future[Unit] = {
      if (d.isCancellable ) {
        val f = execution ? CancelJobCommand(d.context, CancelJobRequest(jobId))
        f.map(_ => ())
      } else Future.successful(())
    }

    val out = for {
      details <- OptionT(jobStatusById(jobId))
      _ <- OptionT.liftF(tryCancel(details))
      // we should do second request to store,
      // because there is correct situation that job can be completed before cancelling
      updated <- OptionT(jobStatusById(jobId))
    } yield updated
    out.value
  }

  private def askManager[T: ClassTag](msg: Any): Future[T] = typedAsk[T](execution, msg)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any): Future[T] = ref.ask(msg).mapTo[T]

}

