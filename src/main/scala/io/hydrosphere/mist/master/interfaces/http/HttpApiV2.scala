package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.server.{Directives, Route}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.{JobStartRequest, RunMode, RunSettings}
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps

/**
  * New http api
  *
  * jobs:
  *   - list available endpoints: GET /v2/api/jobs/endpoints
  *
  *   - run job: POST /v2/api/jobs/{endpoint-id}
  *     POST DATA: jobs args as map { "arg-name": "arg-value", ...}
  *     returns `{"id": "$job-run-id"}`
  *
  *     query params(not required, experimental, NOT FOR UI):
  *
  *       - externalId: additional job id
  *
  *       - context: set not default `namespace`/`spark context`
  *
  *       - mode: how to run worker for that job:
  *         - default: one worker for all job in namespace/sparkcontext
  *         - uniqContext: one worker per job
  *
  *       - uniqWorkerId: if mode id `uniqContext` worker has that marker in id
  *
  *   - job execution status: GET /v2/api/jobs/status/{job-run-id} - returns one object
  *
  *      by external-id(NOT FOR UI) - GET /v2/api/jobs/status/{external-id}?isExternal=true
  *        returns list of job details (there is no guarantee that externalId is unique for all jobs)
  *
  *
  * workers:
  *   list workers - GET - /v2/api/workers
  *   stop worker - DELETE - /v2/api/workers/{id} (output should be changed)
  *
  */
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
    } ~
    path(root / "jobs" / "endpoints" ) {
      get { complete {
        master.listRoutesInfo().map(HttpJobInfo.convert)
      }}
    } ~
    path(root / "workers") {
      get { complete(master.workers()) }
    } ~
    path(root / "workers" / Segment) { workerId =>
      delete { completeU(master.stopWorker(workerId).map(_ => ())) }
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
