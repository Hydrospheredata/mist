package io.hydrosphere.mist.master

import java.io.File

import cats.data._
import cats.implicits._
import io.hydrosphere.mist.master.artifact.ArtifactRepository
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
  val artifactRepository: ArtifactRepository
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

  def loadEndpointInfo(e: EndpointConfig): Future[Option[FullEndpointInfo]] = {
    import e._

    val fileF = JobResolver.fromPath(path) match {
      case _: LocalResolver => artifactRepository.get(FilenameUtils.getName(path))
      case p => Future.successful(if (p.exists) Some(p.resolve()) else None)
    }

    val res = for {
      file <- OptionT(fileF)
      job <- OptionT.liftF(loadInfo(name, file, className))
      info = FullEndpointInfo(e, job)
    } yield info

    res.value
  }

  private def loadInfo(name: String, file: File, className: String): Future[JobInfo] = {
    JobInfo.load(name, file, className) match {
      case Success(f) => Future.successful(f)
      case Failure(ex) => Future.failed(ex)
    }
  }

  def endpointsInfo: Future[Seq[FullEndpointInfo]] = for {
    configs <- endpoints.all
    fullInfos <- Future.sequence(configs.map(loadEndpointInfo))
    infos = fullInfos.flatten
  } yield infos

  def endpointInfo(id: String): Future[Option[FullEndpointInfo]] = {
    val res = for {
      e <- OptionT(endpoints.get(id))
      fullInfo <- OptionT(loadEndpointInfo(e))
    } yield fullInfo
    res.value
  }

}
