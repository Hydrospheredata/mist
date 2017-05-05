package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages
import io.hydrosphere.mist.Messages.StatusMessages.Register
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.models.{JobStartRequest, JobStartResponse}
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class MasterService(
  workerManager: ActorRef,
  statusService: ActorRef,
  jobRoutes: JobRoutes
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(10.second)

  def activeJobs(): Future[List[JobDetails]] = {
    val future = statusService ? StatusMessages.RunningJobs
    future.mapTo[List[JobDetails]]
  }

  def jobStatusById(id: String): Future[Option[JobDetails]] = {
    val f = statusService ? StatusMessages.GetById(id)
    f.mapTo[Option[JobDetails]]
  }

  def jobStatusByExternalId(id: String): Future[Option[JobDetails]] = {
    val f = statusService ? StatusMessages.GetByExternalId(id)
    f.mapTo[Option[JobDetails]]
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

//  def startJob(
//    routeId: String,
//    action: Action,
//    arguments: JobParameters,
//    source: JobDetails.Source,
//    externalId: Option[String]
//  ): Future[JobResult] = {
//    buildParams(routeId, action, arguments, externalId) match {
//      case Some(execParams) =>
//        val request = toRequest(execParams)
//
//        statusService ! Register(request.id, execParams, source)
//
//        val promise = Promise[JobResult]
//        // that timeout only for obtaining execution info
//        implicit val timeout = Timeout(30.seconds)
//
//        workerManager.ask(WorkerCommand(execParams.namespace, request))
//          .mapTo[ExecutionInfo]
//          .flatMap(_.promise.future).onComplete({
//          case Success(r) =>
//            promise.success(JobResult.success(r, execParams))
//          case Failure(e) =>
//            promise.success(JobResult.failure(e.getMessage, execParams))
//        })
//        promise.future
//
//      case None =>
//        Future.failed(new RuntimeException(s"Job with $routeId not found"))
//    }
//  }

//  def startJob(r: JobExecutionRequest, source: JobDetails.Source): Future[JobResult] = {
//    import r._
//    startJob(routeId, action, parameters, source, r.externalId)
//  }

  def runJob(req: JobStartRequest, source: JobDetails.Source): Future[JobStartResponse] = {
    val id = req.routeId
    jobRoutes.getDefinition(id) match {
      case None => Future.failed(new IllegalStateException(s"Job with route $id not defined"))
      case Some(d) =>
        val internalRequest = RunJobRequest(
          id = UUID.randomUUID().toString,
          JobParams(
            filePath = d.path,
            className = d.className,
            arguments = req.parameters,
            action = Action.Execute
          )
        )

        val namespace = req.runSettings.contextId.getOrElse(d.nameSpace)
        val cmd = RunJobCommand(namespace, req.runSettings.mode, internalRequest)

        //TODO: remove fixture
        val executionParams = JobExecutionParams("", "", "", Map.empty, None, None)
        statusService ! Register(internalRequest.id, executionParams, source)

        workerManager.ask(cmd).mapTo[ExecutionInfo].map(_ => JobStartResponse(internalRequest.id))
    }
  }

  def runJob2(req: JobStartRequest, source: JobDetails.Source): Future[ExecutionInfo] = {
    val id = req.routeId
    jobRoutes.getDefinition(id) match {
      case None => Future.failed(new IllegalStateException(s"Job with route $id not defined"))
      case Some(d) =>
        val internalRequest = RunJobRequest(
          id = UUID.randomUUID().toString,
          JobParams(
            filePath = d.path,
            className = d.className,
            arguments = req.parameters,
            action = Action.Execute
          )
        )

        val namespace = req.runSettings.contextId.getOrElse(d.nameSpace)
        val cmd = RunJobCommand(namespace, req.runSettings.mode, internalRequest)

        //TODO: remove fixture
        val executionParams = JobExecutionParams("", "", "", Map.empty, None, None)
        statusService ! Register(internalRequest.id, executionParams, source)

        workerManager.ask(cmd).mapTo[ExecutionInfo]
    }
  }

  def forceJobRun(req: JobStartRequest, source: JobDetails.Source): Future[JobResult] = {
    val promise = Promise[JobResult]
    runJob2(req, source).flatMap(execution => execution.promise.future).onComplete {
      case Success(r) =>
        promise.success(JobResult.success(r, req))
      case Failure(e) =>
        promise.success(JobResult.failure(e.getMessage, req))
    }

    promise.future
  }

//  private def buildParams(
//    routeId: String,
//    action: Action,
//    arguments: JobParameters,
//    externalId: Option[String]
//  ): Option[JobExecutionParams] = {
//    jobRoutes.getDefinition(routeId).map(d => {
//      JobExecutionParams.fromDefinition(
//        definition = d,
//        action = action,
//        parameters = arguments,
//        externalId = externalId
//      )
//    })
//  }

//  private def toRequest(execParams: JobExecutionParams): RunJobRequest = {
//    RunJobRequest(
//      id = UUID.randomUUID().toString,
//      JobParams(
//        filePath = execParams.path,
//        className = execParams.className,
//        arguments = execParams.parameters,
//        action = execParams.action
//      )
//    )
//  }

//  def recoverJobs(): Future[Unit] = {
//    for {
//      jobs <- activeJobs()
//
//    }
//  }

//  def recoverJobs(publishers: Map[Provider, ActorRef]): Unit = {
//    activeJobs().onSuccess({ case jobs =>
//      jobs.foreach(details => {
//        details.source match {
//          case a: Async if publishers.get(a.provider).isDefined =>
//            publishers.get(a.provider).foreach(ref => {
//              logger.info(s"Job $details is restarted")
//              restartAsync(details, ref)
//            })
//          case _ =>
//            logger.info(s"Mark job $details as aborted")
//            statusService ! FailedEvent(
//              details.jobId,
//              System.currentTimeMillis(),
//              "Worker was stopped"
//            )
//        }
//      })
//      logger.info("Job recovery done")
//    })
//  }

//  private def restartAsync(job: JobDetails, publisher: ActorRef): Future[JobResult] = {
//    val future = startJob(
//      //TODO: get!
//      job.configuration.route.get,
//      job.configuration.action,
//      job.configuration.parameters,
//      job.source,
//      job.configuration.externalId)
//
//    future.onComplete({
//      case Success(jobResult) => publisher ! jobResult
//      case Failure(e) =>
//        logger.error(s"Job $job execution failed", e)
//        val msg = s"Job execution failed for $job. Error message ${e.getMessage}"
//        publisher ! msg
//    })
//
//    future
//  }

}
