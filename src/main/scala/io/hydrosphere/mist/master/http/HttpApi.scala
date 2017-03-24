package io.hydrosphere.mist.master.http

import akka.actor.ActorSelection
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import akka.http.scaladsl.server.{Directives, RejectionHandler, Route}
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.master.cluster.ClusterManager
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Future
import scala.language.reflectiveCalls
import scala.reflect.runtime.universe._

trait HttpRoute {

  val route: Route

}

/**
  * Server static ui resources
  */
object HttpUi extends Directives with HttpRoute {

  import StatusCodes._

  private val notFound = RejectionHandler.newBuilder()
    .handleNotFound(complete(HttpResponse(NotFound, entity = "Not found")))
    .result()

  val route: Route = {
    pathPrefix("ui") {
      get {
        pathEnd {
          redirect("/ui/", PermanentRedirect)
        } ~
        pathSingleSlash {
          getFromResource("web/index.html", Default("web/index.html"), getClass.getClassLoader)
        } ~
        handleRejections(notFound) {
          getFromResourceDirectory("web", getClass.getClassLoader)
        }
      }
    }
  }

}

sealed trait HttpJobInfo

case class HttpPyJobInfo(
  isPython: Boolean = true
) extends HttpJobInfo

case class HttpJvmJobInfo(
  execute: Option[Map[String, Type]] = None,
  train: Option[Map[String, Type]] = None,
  serve: Option[Map[String, Type]] = None,

  isHiveJob: Boolean = false,
  isSqlJob: Boolean = false,
  isStreamingJob: Boolean = false,
  isMLJob: Boolean = false
) extends HttpJobInfo


case class JobExecutionStatus(
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized
) {

}

class ClusterMaster(
  managerRef: ActorSelection
) {
  import scala.concurrent.duration._

  implicit val timout = Timeout(1.second)

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
}


class HttpApi(master: ClusterMaster) extends Logger with JsonCodecs {

  import Directives._
  import io.circe.generic.auto._

  val route: Route = {
    pathPrefix("internal") {
      path("jobs") {
        get { complete(master.jobsStatuses()) }
      } ~
      path("workers") {
        get { complete(master.workers()) }
      }
    }
  }
//
//

//  val route: Route = {
//    path("internal" / Segment) { cmd =>
//      get {
//        path("jobs") {
//          complete(A(5))
//          complete(JobStatus())
//        }
//        path("workers") {
//          implicit val timeout = Timeout.durationToTimeout(Constants.CLI.timeoutDuration)
//          val future = system.actorSelection(s"akka://mist/user/${Constants.Actors.clusterManagerName}") ? ClusterManager.GetWorkers()
//          future.recover {
//            case error: Throwable =>
//              HttpResponse(entity = HttpEntity(`application/json`, error.toString))
//          }.map[ToResponseMarshallable] {
//            case list: List[WorkerLink] => HttpResponse(entity = HttpEntity(`application/json`, list.toJson.compactPrint))
//          }
//        } ~
//        path("routers") {
//          complete {
//            try {
//              HttpResponse(entity = HttpEntity(`application/json`, RouteConfig.info.toJson.compactPrint))
//            } catch {
//              case exc: Throwable => HttpResponse(StatusCodes.InternalServerError, entity = HttpEntity(ContentType(MediaTypes.`application/json`), exc.getMessage))
//            }
//          }
//        }
//      }
//    }
//  }
//  import akka.http.scaladsl.marshalling.ToEntityMarshaller
//
//  def completeJson[T](a: T)(implicit m: ToEntityMarshaller[T]): StandardRoute = {
//    StandardRoute(_.complete(m.(e => HttpResponse(entity = e))))
//  }
}
