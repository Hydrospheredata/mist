package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{StopAllWorkers, StopJob, StopWorker}
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.cluster.ClusterManager
import io.hydrosphere.mist.master.namespace.{WorkersManager, Namespace}
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

  implicit val timeout = Timeout(1.second)

  private val activeStatuses = List(JobDetails.Status.Running, JobDetails.Status.Queued)

  private val namespaces = {
    val uniq = jobRoutes.listDefinition().map(_.nameSpace).toSet
    uniq.map(id => id -> new Namespace(id, managerRef2)).toMap
  }

  def activeJobs(): List[JobDetails] = {
    JobRepository().filteredByStatuses(activeStatuses)
  }

  def workers(): Future[List[String]] = {
    val f = managerRef2 ? WorkersManager.GetWorkers
    f.mapTo[List[String]]
  }

  def stopAllWorkers(): Future[Unit] = {
    val f = managerRef ? StopAllWorkers()
    f.map(_ => ())
  }

  //TODO: if job id unknown??
  def stopJob(id: String): Future[Unit] = {
    val f = managerRef ? StopJob(id)
    f.map(_ => ())
  }

  //TODO: if worker id unknown??
  def stopWorker(id: String): Future[String] = {
    managerRef2 ! StopWorker(id)
    Future.successful(id)
  }

  def listRoutesInfo(): Seq[JobInfo] = jobRoutes.listInfos()

  //TODO: why we need full configuration ??
  //TODO: for starting job we need only id, action, and params
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
        logger.info("WTF?")
        Future.failed(new IllegalStateException("WTF"))
    }
  }

}
