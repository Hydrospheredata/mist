package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages
import io.hydrosphere.mist.Messages.StatusMessages.{Register, RunningJobs, UpdateStatus}
import io.hydrosphere.mist.jobs.JobDetails.Source.Async
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class MasterService(
  workerManager: ActorRef,
  statusService: ActorRef,
  jobRoutes: JobRoutes
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(5.second)

  def activeJobs(): Future[List[JobDetails]] = {
    val future = statusService ? StatusMessages.RunningJobs
    future.mapTo[List[JobDetails]]
  }

  def workers(): Future[List[WorkerLink]] = {
    val f = workerManager ? GetWorkers
    f.mapTo[List[WorkerLink]]
  }

  def stopAllWorkers(): Future[Unit] = {
    val f = workerManager ? StopAllWorkers
    f.map(_ => ())
  }

  def stopJob(namespace: String, runId: String): Future[Unit] = {
    val f = workerManager ? WorkerCommand(namespace, CancelJobRequest(runId))
    f.map(_ => ())
  }

  def stopWorker(id: String): Future[String] = {
    workerManager ! StopWorker(id)
    Future.successful(id)
  }

  def listRoutesInfo(): Seq[JobInfo] = jobRoutes.listInfos()
  def routeDefinitions(): Seq[JobDefinition] = jobRoutes.listDefinition()

  def startJob(
    id: String,
    action: Action,
    arguments: JobParameters,
    source: JobDetails.Source,
    externalId: Option[String]
  ): Future[JobResult] = {
    buildParams(id, action, arguments, externalId) match {
      case Some(execParams) =>
        val request = toRequest(execParams)

        statusService ! Register(request.id, execParams, source)

        val promise = Promise[JobResult]
        // that timeout only for obtaining execution info
        implicit val timeout = Timeout(30.seconds)

        workerManager.ask(WorkerCommand(execParams.namespace, request))
          .mapTo[ExecutionInfo]
          .flatMap(_.promise.future).onComplete({
          case Success(r) =>
            promise.success(JobResult.success(r, execParams))
          case Failure(e) =>
            promise.success(JobResult.failure(e.getMessage, execParams))
        })
        promise.future

      case None =>
        Future.failed(new RuntimeException(s"Job with $id not found"))
    }
  }

  def startJob(r: JobExecutionRequest, source: JobDetails.Source): Future[JobResult] = {
    import r._
    startJob(jobId, action, parameters, source, r.externalId)
  }

  private def buildParams(
    routeId: String,
    action: Action,
    arguments: JobParameters,
    externalId: Option[String]
  ): Option[JobExecutionParams] = {
    jobRoutes.getDefinition(routeId).map(d => {
      JobExecutionParams.fromDefinition(
        definition = d,
        action = action,
        parameters = arguments,
        externalId = externalId
      )
    })
  }

  private def toRequest(execParams: JobExecutionParams): RunJobRequest = {
    RunJobRequest(
      id = UUID.randomUUID().toString,
      JobParams(
        filePath = execParams.path,
        className = execParams.className,
        arguments = execParams.parameters,
        action = execParams.action
      )
    )

  }

  //TODO: job results is not publishing into async interface
  def recoverJobs(): Future[Unit] = {
    implicit val timeout = Timeout(30 seconds)
    val f = (statusService ? RunningJobs).mapTo[List[JobDetails]]
    val result = f.map(jobs => {
      jobs.foreach(d => d.source match {
        case a:Async =>
          logger.info(s"Job is recovered $d")
          startJob(
            d.jobId,
            d.configuration.action,
            d.configuration.parameters,
            d.source,
            d.configuration.externalId)
        case _ =>
          logger.info(s"Mark job aborted ${d.jobId}")
          statusService ! UpdateStatus(d.jobId, JobDetails.Status.Aborted, System.currentTimeMillis())
      })
    })
    result.onComplete({
      case Success(_) =>
        logger.info("Job Recovery done")
      case Failure(e) =>
        logger.error("Job recovery failed", e)
    })
    result
  }

}
