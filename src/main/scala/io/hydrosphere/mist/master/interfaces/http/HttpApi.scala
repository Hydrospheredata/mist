package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.server.{Directives, Route}
import io.hydrosphere.mist.api._
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.{RunSettings, JobStartRequest}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.Future
import scala.language.reflectiveCalls

class HttpApi(master: MasterService) extends Logger {

  import Directives._
  import JsonCodecs._
  import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet

  import scala.concurrent.ExecutionContext.Implicits.global

  val route: Route = {
    path("internal" / "jobs") {
      get { complete (master.activeJobs())}
    } ~
    path("internal" / "jobs" / Segment / Segment) { (namespace, jobId) =>
      delete {
        completeU { master.stopJob(namespace, jobId) }
      }
    } ~
    path("internal" / "workers" ) {
      get { complete(master.workers()) }
    } ~
    path("internal" / "workers") {
      delete { completeU {
        master.stopAllWorkers()
      }}
    } ~
    path("internal" / "workers" / Segment) { workerId =>
      delete {
        complete {
          master.stopWorker(workerId).map(_ => Map("id" -> workerId ))
        }
      }
    } ~
    path("internal" / "routers") {
      get {
        complete {
          val result = master.listEndpoints()
           .map(i => i.definition.name -> HttpJobInfo.convert(i))
           .toMap
          result
        }
      }
    } ~
    path("api" / Segment) { routeId =>
      post { parameters('train.?, 'serve.?) { (train, serve) =>
        entity(as[JobParameters]) { jobParams =>

          complete {
            val action = if (train.isDefined)
              Action.Train
            else if (serve.isDefined)
              Action.Serve
            else
              Action.Execute

            val request = JobStartRequest(
              routeId = routeId,
              parameters = jobParams,
              externalId = None,
              runSettings = RunSettings.Default
            )
            master.forceJobRun(request, Source.Http, action)
          }
        }
      }}
    }

  }

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }

}
