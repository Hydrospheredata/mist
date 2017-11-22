package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor._
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.core.jvmjob.FullJobInfo
import io.hydrosphere.mist.master.JobDetails.Source.Async
import io.hydrosphere.mist.master.Messages.JobExecution.CreateContext
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.jobs.JobInfoProviderService
import io.hydrosphere.mist.master.models.RunMode.{ExclusiveContext, Shared}
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}

class MainService(
  val jobService: JobService,
  val endpoints: EndpointsStorage,
  val contexts: ContextsStorage,
  val logsPaths: LogStoragePaths,
  val jobInfoProviderService: JobInfoProviderService,
  //TODO: it is not used here anymore.
  val artifactRepository: ArtifactRepository
) extends Logger {

  implicit val timeout: Timeout = Timeout(5 seconds)

  def runJob(
    req: EndpointStartRequest,
    source: JobDetails.Source
  ): Future[Option[JobStartResponse]] = {
    val out = for {
      executionInfo <- OptionT(runJobRaw(req, source))
    } yield JobStartResponse(executionInfo.request.id)
    out.value
  }

  def forceJobRun(
    req: EndpointStartRequest,
    source: JobDetails.Source,
    action: Action = Action.Execute
  ): Future[Option[JobResult]] = {
    val promise = Promise[Option[JobResult]]
    runJobRaw(req, source, action).map({
      case Some(info) => info.promise.future.onComplete {
        case Success(r) =>
          promise.success(Some(JobResult.success(r)))
        case Failure(e) =>
          promise.success(Some(JobResult.failure(e.getMessage)))
      }
      case None => promise.success(None)
    }).onFailure({
      case e: Throwable => promise.failure(e)
    })

    promise.future
  }

  def devRun(
    req: DevJobStartRequest,
    source: JobDetails.Source,
    action: Action = Action.Execute
  ): Future[ExecutionInfo] = {

    val endpoint = EndpointConfig(
      name = req.fakeName,
      path = req.path,
      className = req.className,
      defaultContext = req.context
    )

    for {
      info          <- jobInfoProviderService.getJobInfoByConfig(endpoint)
      context       <- contexts.getOrDefault(req.context)
      _             <- jobInfoProviderService.validateJobByConfig(endpoint, req.parameters, action)
      runMode       =  selectRunMode(context, info, req.workerId)
      executionInfo <- jobService.startJob(JobStartRequest(
        id = UUID.randomUUID().toString,
        endpoint = info,
        context = context,
        parameters = req.parameters,
        runMode = runMode,
        source = source,
        externalId = req.externalId,
        action = action
      ))
    } yield executionInfo
  }

  private def selectRunMode(
    config: ContextConfig,
    info: FullJobInfo,
    workerId: Option[String]
  ): RunMode =
    if (info.isStreamingJob) ExclusiveContext(workerId)
    else config.workerMode match {
      case "exclusive" => ExclusiveContext(workerId)
      case "shared" => Shared
      case _ =>
        throw new IllegalArgumentException(s"unknown worker run mode ${config.workerMode} for context ${config.name}")
    }

  def recoverJobs(): Future[Unit] = {

    def restartJob(job: JobDetails): Future[Unit] = {
      val req = EndpointStartRequest(job.endpoint, job.params.arguments, job.externalId, id = job.jobId)
      runJob(req, job.source).map(_ => ())
    }

    def failOrRestart(d: JobDetails): Future[Unit] = d.source match {
      case a: Async => restartJob(d)
      case _ =>
        logger.info(s"Mark job $d as failed")
        jobService.markJobFailed(d.jobId, "Worker was stopped")
    }

    jobService.activeJobs().flatMap(notCompleted => {
      val processed = notCompleted.map(d => failOrRestart(d).recoverWith {
        case e: Throwable =>
          logger.error(s"Error occurred during recovering ${d.jobId}", e)
          Future.successful(())
      })
      Future.sequence(processed)
    }).map(_ => ())
  }

  private def runJobRaw(
    req: EndpointStartRequest,
    source: JobDetails.Source,
    action: Action = Action.Execute): Future[Option[ExecutionInfo]] = {
    val out = for {
      info         <- OptionT(jobInfoProviderService.getJobInfo(req.endpointId))
      _            <- OptionT.liftF(jobInfoProviderService.validateJob(req.endpointId, req.parameters, action))
      context      <- OptionT.liftF(selectContext(req, info.defaultContext))
      runMode      =  selectRunMode(context, info, req.runSettings.workerId)
      jobStartReq  =  JobStartRequest(
        id = req.id,
        endpoint = info,
        context = context,
        parameters = req.parameters,
        runMode = runMode,
        source = source,
        externalId = req.externalId,
        action = action
      )
      executionInfo <- OptionT.liftF(jobService.startJob(jobStartReq))
    } yield executionInfo

    out.value
  }

  private def selectContext(req: EndpointStartRequest, context: String): Future[ContextConfig] = {
    val name = req.runSettings.contextId.getOrElse(context)
    contexts.getOrDefault(name)
  }

  def endpointsInfo: Future[Seq[FullJobInfo]] = for {
    configs <- endpoints.all
    infos   <- Future.sequence(configs.map(e => {
      jobInfoProviderService.getJobInfoByConfig(e).map(Some.apply)
        .recover {
          case ex =>
            logger.error(ex.getMessage, ex)
            None
        }
    }))
  } yield infos.flatten

}

object MainService extends Logger {

  def start(
    jobService: JobService,
    endpoints: EndpointsStorage,
    contexts: ContextsStorage,
    logsPaths: LogStoragePaths,
    jobInfoProvider: JobInfoProviderService,
    artifactRepository: ArtifactRepository
  ): Future[MainService] = {
    val service = new MainService(jobService, endpoints, contexts, logsPaths, jobInfoProvider, artifactRepository)
    for {
      precreated <- contexts.precreated
      _ = precreated.foreach(ctx => {
        logger.info(s"Precreate context for ${ctx.name}")
        jobService.workerManager ! CreateContext(ctx)
      })
      _ <- service.recoverJobs()
    } yield service
  }
}