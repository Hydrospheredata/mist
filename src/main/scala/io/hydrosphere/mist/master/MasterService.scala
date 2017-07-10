package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages
import io.hydrosphere.mist.Messages.StatusMessages.{FailedEvent, Register}
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.jobs.JobDetails.Source.Async
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.models.{JobStartRequest, JobStartResponse}
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class MasterService(
  workerManager: ActorRef,
  statusService: ActorRef,
  jobEndpoints: JobEndpoints,
  val contextsSettings: ContextsSettings
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.second)

  def activeJobs(): Future[Seq[JobDetails]] = {
    val future = statusService ? StatusMessages.RunningJobs
    future.mapTo[Seq[JobDetails]]
  }

  def jobStatusById(id: String): Future[Option[JobDetails]] = {
    val f = statusService ? StatusMessages.GetById(id)
    f.mapTo[Option[JobDetails]]
  }

  def endpointHistory(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    val f = statusService ? StatusMessages.GetEndpointHistory(id, limit, offset, statuses)
    f.mapTo[Seq[JobDetails]]
  }

  def getHistory(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    val f = statusService ? StatusMessages.GetHistory(limit, offset, statuses)
    f.mapTo[Seq[JobDetails]]
  }

  def workers(): Future[Seq[WorkerLink]] = {
    val f = workerManager ? GetWorkers
    f.mapTo[Seq[WorkerLink]]
  }

  def stopAllWorkers(): Future[Unit] = {
    val f = workerManager ? StopAllWorkers
    f.map(_ => ())
  }

  def stopJob(jobId: String): Future[Option[JobDetails]] = {
    def tryCancel(d: JobDetails): Future[Unit] = d.workerId match {
      case Some(id) if d.isCancellable =>
        val f = workerManager ? CancelJobCommand(id, CancelJobRequest(jobId))
        f.map(_ => ())
      case _ => Future.successful(())
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


  def stopWorker(id: String): Future[String] = {
    workerManager ! StopWorker(id)
    Future.successful(id)
  }

  def endpointInfo(id: String): Option[JobInfo] = jobEndpoints.getInfo(id)

  def listEndpoints(): Seq[JobInfo] = jobEndpoints.listInfos()

  def routeDefinitions(): Seq[JobDefinition] = jobEndpoints.listDefinition()

  def startJob(
    req: JobStartRequest,
    source: JobDetails.Source,
    action: Action = Action.Execute
  ): Future[ExecutionInfo] = {
    val id = req.endpointId
    jobEndpoints.getDefinition(id) match {
      case None => Future.failed(new IllegalStateException(s"Job with route $id not defined"))
      case Some(d) =>
        val internalRequest = RunJobRequest(
          id = req.id,
          JobParams(
            filePath = d.path,
            className = d.className,
            arguments = req.parameters,
            action = action
          )
        )

        val namespace = req.runSettings.contextId.getOrElse(d.nameSpace)

        val registrationCommand = Register(
          request = internalRequest,
          endpoint = req.endpointId,
          context = namespace,
          source = source,
          req.externalId
        )
        val startCmd = RunJobCommand(namespace, req.runSettings.mode, internalRequest)

        for {
          _ <- statusService.ask(registrationCommand).mapTo[Unit]
          info <- workerManager.ask(startCmd).mapTo[ExecutionInfo]
        } yield info
    }
  }

  def runJob(
    req: JobStartRequest,
    source: JobDetails.Source,
    action: Action = Action.Execute
  ): Future[JobStartResponse] = {
    startJob(req, source, action).map(info => JobStartResponse(info.request.id))
  }

  def forceJobRun(
    req: JobStartRequest,
    source: JobDetails.Source,
    action: Action
  ): Future[JobResult] = {
    val promise = Promise[JobResult]
    startJob(req, source, action).flatMap(execution => execution.promise.future).onComplete {
      case Success(r) =>
        promise.success(JobResult.success(r, req))
      case Failure(e) =>
        promise.success(JobResult.failure(e.getMessage, req))
    }

    promise.future
  }

  def recoverJobs(): Unit = {
    activeJobs().onSuccess({ case jobs =>
      jobs.foreach(details => {
        details.source match {
          case a: Async => restartJob(details)
          case _ =>
            logger.info(s"Mark job $details as aborted")
            statusService ? FailedEvent(
              details.jobId,
              System.currentTimeMillis(),
              "Worker was stopped"
            )
        }
      })
      logger.info("Job recovery done")
    })
  }

  private def restartJob(job: JobDetails): Unit = {
    val req = JobStartRequest(job.endpoint, job.params.arguments, job.externalId, id = job.jobId)
    runJob(req, job.source, Action.Execute)
  }

}
