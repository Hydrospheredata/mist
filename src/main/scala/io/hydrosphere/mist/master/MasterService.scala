package io.hydrosphere.mist.master

import cats.data._
import cats.implicits._
import io.hydrosphere.mist.jobs.JobDetails.Source.Async
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.{Future, Promise}
import scala.util.{Try, Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class MasterService(
  val jobService: JobService,
  val endpoints: EndpointsStorage,
  val contexts: ContextsStorage,
  val logStorageMappings: LogStorageMappings
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
      fullInfo       <- OptionT(endpoints.getFullInfo(req.endpointId))
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
