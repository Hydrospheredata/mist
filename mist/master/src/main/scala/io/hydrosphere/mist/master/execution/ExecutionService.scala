package io.hydrosphere.mist.master.execution

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, JobParams, RunJobRequest}
import io.hydrosphere.mist.master.Messages.StatusMessages.InitializedEvent
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.WorkerHub
import io.hydrosphere.mist.master.logging.LogService
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.store.JobRepository
import io.hydrosphere.mist.master.{EventsStreamer, JobDetails, JobDetailsRequest, JobDetailsResponse}
import io.hydrosphere.mist.utils.akka.ActorF

import scala.concurrent.Future

/**
  *  Jobs starting/stopping, statuses, worker utility methods
  */
class ExecutionService(
  contextsMaster: ActorRef,
  workersHub: WorkerHub,
  statusReporter: StatusReporter,
  repo: JobRepository
) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  private implicit val timeout = Timeout(10.second)

  def activeJobs(): Future[Seq[JobDetails]] = repo.running()

  def markJobFailed(id: String, reason: String): Future[Unit] = {
    repo.path(id)(d => d.withEndTime(System.currentTimeMillis()).withFailure(reason).withStatus(JobDetails.Status.Failed))
  }

  def jobStatusById(id: String): Future[Option[JobDetails]] = repo.get(id)

  def getHistory(req: JobDetailsRequest): Future[JobDetailsResponse] = repo.getJobs(req)

  def workers(): Seq[WorkerLink] = workersHub.workerConnections().map(_.data)

  def getWorkerLink(workerId: String): Option[WorkerLink] = {
    workersHub.workerConnection(workerId).map(_.data)
  }


  def stopAllWorkers(): Future[Unit] = workersHub.shutdownAllWorkers()

  def stopWorker(id: String): Future[Unit] = workersHub.shutdownWorker(id)


  def startJob(req: JobStartRequest): Future[ExecutionInfo] = {
    import req._

    val internalRequest = RunJobRequest(
      id = id,
      params = JobParams(
        filePath = function.path,
        className = function.className,
        arguments = parameters,
        action = action
      ),
      startTimeout = req.timeouts.start,
      timeout = req.timeouts.perform
    )

    val startCmd = ContextEvent.RunJobCommand(context, internalRequest)

    val details = JobDetails(
      function.name,
      req.id,
      internalRequest.params,
      context.name,
      externalId, source)
    for {
      _    <- repo.update(details)
      _    = statusReporter.reportPlain(InitializedEvent(internalRequest.id, internalRequest.params, req.externalId, function.name, context.name))
      info <- contextsMaster.ask(startCmd).mapTo[ExecutionInfo]
    } yield info
  }

  def stopJob(jobId: String): Future[Option[JobDetails]] = {
    import cats.data._
    import cats.implicits._

    def tryCancel(d: JobDetails): Future[JobDetails] = {
      if (d.isCancellable ) {
        val f = contextsMaster ? ContextEvent.CancelJobCommand(d.context, CancelJobRequest(jobId))
        f.mapTo[ContextEvent.JobCancelledResponse].map(r => r.details)
      } else Future.successful(d)
    }

    val out = for {
      details <- OptionT(jobStatusById(jobId))
      updated <- OptionT.liftF(tryCancel(details))
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
      val props = ContextFrontend.props(ctx.name, reporter, logService, hub.start)
      val ref = af.actorOf(props)
      ref ! ContextEvent.UpdateContext(ctx)
      ref
    })
    val contextsMaster = system.actorOf(ContextsMaster.props(mkContext))
    new ExecutionService(contextsMaster, hub, reporter, repo)
  }

}

