package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.server.{Directives, Route}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.{JobStartRequest, RunMode, RunSettings}
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.Future
import scala.language.postfixOps

class HttpApiV2(master: MasterService) {

  import HttpApiV2._
  import Directives._
  import JsonCodecs._
  import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet

  private val root = "v2" / "api"
  private val postJobQuery =
    parameters(
      'externalId ?,
      'context ?,
      'mode ? ,
      'uniqWorkerId ?
    ).as(JobRunQueryParams)

  val route: Route = {
    path(root / "jobs" / Segment) { routeId: String =>
      post( postJobQuery { query =>
          entity(as[JobParameters]) { params =>
            val runReq = buildStartRequest(routeId, query, params)
            complete(master.runJob(runReq, Source.Http))
          }
      })
    } ~
    path(root / "jobs" / "status" / Segment) { jobId: String =>
      get( parameter('isExternal.as[Boolean] ? false) { isExternalId =>
        rejectEmptyResponse {
          complete {
            if (isExternalId)
              master.jobStatusByExternalId(jobId)
           else
              master.jobStatusById(jobId)
          }
        }
      })
    }

  }

  private def buildStartRequest(
    routeId: String,
    queryParams: JobRunQueryParams,
    parameters: JobParameters
  ): JobStartRequest = {
    val runSettings = queryParams.buildRunSettings()
    JobStartRequest(routeId, parameters, queryParams.externalId, runSettings)
  }

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }
}

object HttpApiV2 {

  case class JobRunQueryParams(
    externalId: Option[String],
    context: Option[String],
    mode: Option[String],
    uniqWorkerId: Option[String]
  ) {

    def buildRunSettings(): RunSettings = {
      val runMode = mode.flatMap(RunMode.fromString)
          .getOrElse(RunMode.Default) match {
        case u: RunMode.UniqueContext => u.copy(uniqWorkerId)
        case x => x
      }
      RunSettings(context, runMode)
    }
  }


}
