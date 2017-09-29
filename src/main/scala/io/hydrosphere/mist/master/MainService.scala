package io.hydrosphere.mist.master

import java.util.UUID

import cats.data._
import cats.implicits._
import io.hydrosphere.mist.Messages.WorkerMessages.CreateContext
import io.hydrosphere.mist.api.StreamingSupport
import io.hydrosphere.mist.jobs.JobDetails.Source.Async
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.jar.JobClass
import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.logging.LogStoragePaths
import io.hydrosphere.mist.master.models.RunMode.{ExclusiveContext, Shared}
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class MainService(
  val jobService: JobService,
  val endpoints: EndpointsStorage,
  val contexts: ContextsStorage,
  val logsPaths: LogStoragePaths
) extends Logger {

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

    def getInfo(endpoint: EndpointConfig): FullEndpointInfo =
      loadEndpointInfo(endpoint) match {
        case Success(fullInfo) => fullInfo
        case Failure(e) => throw e
      }

    for {
      context       <- contexts.getOrDefault(req.context)
      fullInfo      =  getInfo(endpoint)
      _             <- validate(fullInfo.info, req.parameters, action)
      runMode       =  selectRunMode(context, req.workerId, fullInfo)
      executionInfo <- jobService.startJob(JobStartRequest(
        id = UUID.randomUUID().toString,
        endpoint = endpoint,
        context = context,
        parameters = req.parameters,
        runMode = runMode,
        source = source,
        externalId = req.externalId,
        action = action
      ))
    } yield executionInfo
  }

  private def selectRunMode(config: ContextConfig, workerId: Option[String], fullInfo: FullEndpointInfo): RunMode = {
    def isStreamingJob(jobClass: JobClass) =
      jobClass.supportedClasses().contains(classOf[StreamingSupport])

    def workerModeFromContextConfig = config.workerMode match {
      case "exclusive" => ExclusiveContext(workerId)
      case "shared" => Shared
      case _ =>
        throw new IllegalArgumentException(s"unknown worker run mode ${config.workerMode} for context ${config.name}")
    }

    fullInfo.info match {
      case JvmJobInfo(jobClass) if isStreamingJob(jobClass) =>
        ExclusiveContext(workerId)
      case _ => workerModeFromContextConfig
    }
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
      fullInfo       <- OptionT(endpoints.getFullInfo(req.endpointId))
      endpoint       =  fullInfo.config
      jobInfo        =  fullInfo.info
      _              <- OptionT.liftF(validate(jobInfo, req.parameters, action))
      context        <- OptionT.liftF(selectContext(req, endpoint))
      runMode        =  selectRunMode(context, req.runSettings.workerId, fullInfo)
      jobStartReq    =  JobStartRequest(
        id = req.id,
        endpoint = endpoint,
        context = context,
        parameters = req.parameters,
        runMode = runMode,
        source = source,
        externalId = req.externalId,
        action = action
      )
      executionInfo  <- OptionT.liftF(jobService.startJob(jobStartReq))
    } yield executionInfo

    out.value
  }

  private def selectContext(req: EndpointStartRequest, endpoint: EndpointConfig): Future[ContextConfig] = {
    val name = req.runSettings.contextId.getOrElse(endpoint.defaultContext)
    contexts.getOrDefault(name)
  }

  def validate(jobInfo: JobInfo, params: Map[String, Any], action: Action): Future[Unit] = {
    jobInfo.validateAction(params, action) match {
      case Left(e) => Future.failed(e)
      case Right(_) => Future.successful(())
    }
  }

  def loadEndpointInfo(e: EndpointConfig): Try[FullEndpointInfo] = {
    import e._
    JobInfo.load(name, path, className).map(i => FullEndpointInfo(e, i))
  }

  private def toFullInfo(e: EndpointConfig): Option[FullEndpointInfo] = {
    loadEndpointInfo(e) match {
      case Success(fullInfo) => Some(fullInfo)
      case Failure(err) =>
        logger.error("Invalid route configuration", err)
        None
    }
  }

  def endpointsInfo: Future[Seq[FullEndpointInfo]] = {
    endpoints.all.map(_.flatMap(toFullInfo))
  }

  def endpointInfo(id: String): Future[Option[FullEndpointInfo]] = {
    endpoints.get(id).map(_.flatMap(toFullInfo))
  }

}

object MainService extends Logger {

  def start(
    jobService: JobService,
    endpoints: EndpointsStorage,
    contexts: ContextsStorage,
    logsPaths: LogStoragePaths
  ): Future[MainService] = {
    val service = new MainService(jobService, endpoints, contexts, logsPaths)
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
