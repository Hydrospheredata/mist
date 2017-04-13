package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias._

import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class MasterService(
  workerManager: ActorRef,
  val jobRoutes: JobRoutes
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(5.second)

  def activeJobs(): Future[List[JobExecutionStatus]] = {
    val future = workerManager ? GetActiveJobs
    future.mapTo[List[JobExecutionStatus]]
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

  def startJob(id: String, action: Action, arguments: JobParameters): Future[JobResult] = {
    buildParams(id, action, arguments) match {
      case Some(execParams) =>
        val request = toRequest(execParams)

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

  def startJob(r: JobExecutionRequest): Future[JobResult] =
    startJob(r.jobId, r.action, r.parameters)

  private def buildParams(
    routeId: String,
    action: Action,
    arguments: JobParameters): Option[JobExecutionParams] = {
    jobRoutes.getDefinition(routeId).map(d => {
      JobExecutionParams.fromDefinition(
        definition = d,
        action = action,
        parameters = arguments
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

}
