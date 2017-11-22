package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.server.{Directives, Route}
import io.hydrosphere.mist.api._
import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.master.{JobDetails, MainService}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{EndpointStartRequest, RunSettings}
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.Future
import scala.language.reflectiveCalls

class HttpApi(master: MainService) extends Logger {

  import Directives._
  import JsonCodecs._
  import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet

  import scala.concurrent.ExecutionContext.Implicits.global

  val route: Route = {
    path("internal" / "jobs") {
      get { complete (master.jobService.activeJobs())}
    } ~
    path("internal" / "jobs" / Segment / Segment) { (namespace, jobId) =>
      delete {
        completeU { master.jobService.stopJob(jobId).map(_ => ()) }
      }
    } ~
    path("internal" / "workers" ) {
      get { complete(master.jobService.workers()) }
    } ~
    path("internal" / "workers") {
      delete { completeU {
        master.jobService.stopAllWorkers()
      }}
    } ~
    path("internal" / "workers" / Segment) { workerId =>
      delete {
        complete {
          master.jobService.stopWorker(workerId).map(_ => Map("id" -> workerId ))
        }
      }
    } ~
    path("internal" / "routers") {
      get {
        complete {
          val result = master.endpointsInfo

           .map(seq => seq.map(i => i.name -> HttpJobInfo.convert(i)).toMap)
          result
        }
      }
    } ~
    path("api" / Segment) { routeId =>
      post { parameters('serve.?) { (serve) =>
        entity(as[Map[String, Any]]) { jobParams =>

          complete {
            val action = if (serve.isDefined) Action.Serve else Action.Execute

            val request = EndpointStartRequest(
              endpointId = routeId,
              parameters = jobParams,
              externalId = None,
              runSettings = RunSettings.Default
            )
            master.forceJobRun(request, JobDetails.Source.Http, action)
          }
        }
      }}
    }

  }

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }

}
