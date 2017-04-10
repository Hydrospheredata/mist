package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{StopAllWorkers, StopJob, StopWorker}
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.cluster.ClusterManager
import io.hydrosphere.mist.master.http.JobExecutionStatus
import io.hydrosphere.mist.master.namespace.JobMessages.CancelJobRequest
import io.hydrosphere.mist.master.namespace.NamespaceJobExecutor.GetActiveJobs
import io.hydrosphere.mist.master.namespace.WorkerMessages.WorkerCommand
import io.hydrosphere.mist.master.namespace.{WorkerMessages, WorkersManager, Namespace}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias._

import scala.concurrent.Future

class MasterService(
  managerRef: ActorRef,
  managerRef2: ActorRef,
  val jobRoutes: JobRoutes,
  system: ActorSystem
) extends Logger {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(5.second)

  private val namespaces = {
    val uniq = jobRoutes.listDefinition().map(_.nameSpace).toSet
    uniq.map(id => id -> new Namespace(id, managerRef2)).toMap
  }

  def activeJobs(): Future[List[JobExecutionStatus]] = {
    val future = managerRef2 ? GetActiveJobs
    future.mapTo[List[JobExecutionStatus]]
  }

  def workers(): Future[List[String]] = {
    val f = managerRef2 ? WorkerMessages.GetWorkers
    f.mapTo[List[String]]
  }

  def stopAllWorkers(): Future[Unit] = {
    val f = managerRef ? StopAllWorkers()
    f.map(_ => ())
  }

  def stopJob(namespace: String, runId: String): Future[Unit] = {
    logger.info(s"TRY STOP JOB $namespace $runId")
    val f = managerRef2 ? WorkerCommand(namespace, CancelJobRequest(runId))
    f.map(_ => ())
  }

  //TODO: if worker id unknown??
  def stopWorker(id: String): Future[String] = {
    managerRef2 ! StopWorker(id)
    Future.successful(id)
  }

  def listRoutesInfo(): Seq[JobInfo] = jobRoutes.listInfos()

  def startJob(id: String, action: Action, params: JobParameters): Future[JobResult] = {
    jobRoutes.getDefinition(id) match {
      case Some(d) =>
        val execParams = JobExecutionParams.fromDefinition(
          definition = d,
          action = action,
          parameters = params
        )
        startJob(execParams)
      case None =>
        Future.failed(new RuntimeException(s"Job with $id not found"))
    }
  }

  private def startJob(execParams: JobExecutionParams): Future[JobResult] = {
    namespaces.get(execParams.namespace) match {
      case Some(n) => n.startJob(execParams)
      case None =>
        Future.failed(new IllegalStateException("WTF"))
    }
  }

}
