package io.hydrosphere.mist.master.execution

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, JobParams, RunJobRequest}
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.WorkerHub
import io.hydrosphere.mist.master.logging.LogService
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.store.JobRepository
import io.hydrosphere.mist.master.{EventsStreamer, JobDetails}
import io.hydrosphere.mist.utils.akka.ActorF

import scala.concurrent.Future

/**
  *  Jobs starting/stopping, statuses, worker utility methods
  */
class ExecutionService(
  contextsMaster: ActorRef,
  workersHub: WorkerHub,
  repo: JobRepository
) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.second)

  def activeJobs(): Future[Seq[JobDetails]] = repo.running()

  def markJobFailed(id: String, reason: String): Future[Unit] = {
    repo.path(id)(d => d.withEndTime(System.currentTimeMillis()).withFailure(reason).withStatus(JobDetails.Status.Failed))
  }

  def jobStatusById(id: String): Future[Option[JobDetails]] = repo.get(id)

  def functionJobHistory(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] =
    repo.getByFunctionId(id, limit, offset, statuses)

  def getHistory(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] =
    repo.getAll(limit, offset, statuses)

  def workers(): Seq[WorkerLink] = workersHub.workerConnections().map(_.data)

  def workerByJobId(jobId: String): Future[Option[WorkerLink]] = {
    val res = for {
      job      <- OptionT(jobStatusById(jobId))
      workerId <- OptionT.fromOption[Future](job.workerId)
      conn     <- OptionT.fromOption[Future](workersHub.workerConnection(workerId))
    } yield conn.data
    res.value
  }

  def getWorkerInfo(workerId: String): Future[Option[WorkerFullInfo]] = {

    def mkFullInfo(link: WorkerLink, jobs: Seq[JobDetails]): WorkerFullInfo = {
      WorkerFullInfo(
        name = link.name,
        address = link.address,
        sparkUi = link.sparkUi,
        jobs = jobs.map(toJobLinks),
        initInfo = link.initInfo
      )
    }

    workersHub.workerConnection(workerId) match {
      case Some(conn) => repo.getByWorkerId(workerId).map(jobs => mkFullInfo(conn.data, jobs).some)
      case None => Future.successful(None)
    }
  }

  private def toJobLinks(job: JobDetails): JobDetailsLink = JobDetailsLink(
      job.jobId, job.source, job.startTime, job.endTime,
      job.status, job.function, job.workerId, job.createTime
  )

  def stopAllWorkers(): Future[Unit] = workersHub.shutdownAllWorkers()

  def stopWorker(id: String): Future[Unit] = workersHub.shutdownWorker(id)


  def startJob(req: JobStartRequest): Future[ExecutionInfo] = {
    import req._

    val internalRequest = RunJobRequest(
      id = id,
      JobParams(
        filePath = function.path,
        className = function.className,
        arguments = parameters,
        action = action
      )
    )

    val startCmd = ContextEvent.RunJobCommand(context, internalRequest)

    val details = JobDetails(
      function.name,
      req.id,
      internalRequest.params,
      context.name,
      externalId, source)
    for {
      _ <- repo.update(details)
      info <- contextsMaster.ask(startCmd).mapTo[ExecutionInfo]
    } yield info
  }

  def stopJob(jobId: String): Future[Option[JobDetails]] = {
    import cats.data._
    import cats.implicits._

    def tryCancel(d: JobDetails): Future[Unit] = {
      if (d.isCancellable ) {
        val f = contextsMaster ? ContextEvent.CancelJobCommand(d.context, CancelJobRequest(jobId))
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

  def updateContext(ctx: ContextConfig): Unit =
    contextsMaster ! ContextEvent.UpdateContext(ctx)

}

object ExecutionService {

  def apply(
    spawn: SpawnSettings,
    system: ActorSystem,
    streamer: EventsStreamer,
    repo: JobRepository,
    logService: LogService
  ): ExecutionService = {
    val hub = WorkerHub(spawn, system)
    val reporter = StatusReporter.reporter(repo, streamer, logService)(system)

    val mkContext = ActorF[ContextConfig]((ctx, af) => {
      val props = ContextFrontend.props(ctx.name, reporter, hub.start)
      val ref = af.actorOf(props)
      ref ! ContextEvent.UpdateContext(ctx)
      ref
    })
    val contextsMaster = system.actorOf(ContextsMaster.props(mkContext))
    new ExecutionService(contextsMaster, hub, repo)
  }

}

