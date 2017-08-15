package io.hydrosphere.mist.master

import java.io.File

import cats.data._
import cats.implicits._
import io.hydrosphere.mist.master.artifact.{ArtifactRepository, EndpointArtifactKeyProvider, ArtifactKeyProvider}
import io.hydrosphere.mist.jobs.JobDetails.Source.Async
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.resolvers.{JobResolver, LocalResolver}
import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger
import org.apache.commons.io.FilenameUtils

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class MasterService(
  val jobService: JobService,
  val endpoints: EndpointsStorage,
  val contexts: ContextsStorage,
  val logStorageMappings: LogStorageMappings,
  val artifactRepository: ArtifactRepository,
  val artifactKeyProvider: ArtifactKeyProvider[EndpointConfig, String]
) extends Logger {


  def runJob(req: JobStartRequest, source: JobDetails.Source): Future[Option[JobStartResponse]] = {
    val out = for {
      executionInfo <- OptionT(runJobRaw(req, source))
    } yield JobStartResponse(executionInfo.request.id)
    out.value
  }

  def forceJobRun(req: JobStartRequest, source: JobDetails.Source, action: Action = Action.Execute): Future[Option[JobResult]] = {
    val promise = Promise[Option[JobResult]]
    runJobRaw(req, source, action).map({
      case Some(info) => info.promise.future.onComplete {
        case Success(r) =>
          promise.success(Some(JobResult.success(r, req)))
        case Failure(e) =>
          promise.success(Some(JobResult.failure(e.getMessage, req)))
      }
      case None => promise.success(None)
    })

    promise.future
  }

  def recoverJobs(): Future[Unit] = {

    def restartJob(job: JobDetails): Future[Unit] = {
      val req = JobStartRequest(job.endpoint, job.params.arguments, job.externalId, id = job.jobId)
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
    req: JobStartRequest,
    source: JobDetails.Source,
    action: Action = Action.Execute): Future[Option[ExecutionInfo]] = {
    val out = for {
      fullInfo       <- OptionT(endpointInfo(req.endpointId))
      endpoint       = fullInfo.config
      jobInfo        = fullInfo.info
      _              <- OptionT.liftF(validate(jobInfo, req.parameters, action))
      context        <- OptionT.liftF(selectContext(req, endpoint))
      executionInfo  <- OptionT.liftF(jobService.startJob(
        req.id,
        endpoint,
        context,
        req.parameters,
        req.runSettings.mode,
        source,
        req.externalId,
        action
      ))
    } yield executionInfo

    out.value
  }

  private def selectContext(req: JobStartRequest, endpoint: EndpointConfig): Future[ContextConfig] = {
    val name = req.runSettings.contextId.getOrElse(endpoint.defaultContext)
    contexts.getOrDefault(name)
  }

  def validate(jobInfo: JobInfo, params: Map[String, Any], action: Action): Future[Unit] = {
    jobInfo.validateAction(params, action) match {
      case Left(e) => Future.failed(e)
      case Right(_) => Future.successful(())
    }
  }

  def loadEndpointInfo(e: EndpointConfig): Try[FullEndpointInfo] = for {
    file <- artifactByKey(artifactKeyProvider.provideKey(e))
    jobInfo <- JobInfo.load(e.name, file, e.className)
    fullInfo = FullEndpointInfo(e, jobInfo)
  } yield fullInfo

  private def artifactByKey(artifactKey: String): Try[File] = {
    artifactRepository.get(artifactKey) match {
      case Some(file) => Success(file)
      case None => Failure(new IllegalArgumentException(s"file not found by key $artifactKey"))
    }
  }

  def endpointsInfo: Future[Seq[FullEndpointInfo]] = for {
    configs <- endpoints.all
    fullInfo = configs.map(loadEndpointInfo)
      .foldLeft(List.empty[FullEndpointInfo]) {
        case (list, Success(x)) => list :+ x
        case (list, Failure(ex)) => list
      }
  } yield fullInfo

  def endpointInfo(id: String): Future[Option[FullEndpointInfo]] = {
    val res = for {
      endpoint <- OptionT(endpoints.get(id))
      fullInfo <- OptionT.liftF(loadInfoByEndpoint(endpoint))
    } yield fullInfo

    res.value
  }

  private def loadInfoByEndpoint(endpoint: EndpointConfig): Future[FullEndpointInfo] =
    loadEndpointInfo(endpoint) match {
      case Success(i) => Future.successful(i)
      case Failure(ex) => Future.failed(ex)
    }

}
