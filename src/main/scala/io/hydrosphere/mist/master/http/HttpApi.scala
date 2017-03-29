package io.hydrosphere.mist.master.http

import akka.actor.{ActorRef, ActorSelection}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{StopJob, StopWorker}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.master.cluster.ClusterManager
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Future
import scala.language.reflectiveCalls

case class JobExecutionStatus(
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized
)

class MasterService(managerRef: ActorRef) {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(1.second)

  private val activeStatuses = List(JobDetails.Status.Running, JobDetails.Status.Queued)

  def jobsStatuses(): Map[String, JobExecutionStatus] = {
    JobRepository().filteredByStatuses(activeStatuses)
      .map(d => d.jobId -> JobExecutionStatus(d.startTime, d.endTime, d.status))
      .toMap
  }

  def workers(): Future[List[WorkerLink]] = {
    val f = managerRef ? ClusterManager.GetWorkers()
    f.mapTo[List[WorkerLink]]
  }

  //TODO: if job id unknown??
  def stopJob(id: String): Future[Unit] = {
    val f = managerRef ? StopJob(id)
    f.map(_ => ())
  }

  //TODO: if worker id unknown??
  def stopWorker(id: String): Future[Unit] = {
    val f = managerRef ? StopWorker(id)
    f.map(_ => ())
  }
}


class HttpApi(master: MasterService) extends Logger with JsonCodecs {

  import Directives._

  val route: Route = {
    path("internal" / "jobs") {
      get { complete(master.jobsStatuses()) }
    } ~
    path("internal" / "jobs" / Segment) { jobId =>
      delete {
        completeU { master.stopJob(jobId) }
      }
    } ~
    path("internal" / "workers") {
      get { complete(master.workers()) }
    } ~
    path("internal" / "workers"/ Segment) { workerId =>
      delete {
        completeU { master.stopWorker(workerId) }
      }
    }// ~
//    path("internal" / "routers") {
//      get {}
//    }
  }

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }

}
