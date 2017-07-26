package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.directives.ParameterDirectives
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobResult}
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models.{JobStartRequest, JobStartResponse, RunMode, RunSettings}
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * New http api
  *
  * endpoint list          - GET /v2/api/endpoints
  *
  * endpoint               - GET /v2/api/endpoints/{id}
  *
  * start job              - POST /v2/api/endpoints/{id}
  *                          POST DATA: jobs args as map { "arg-name": "arg-value", ...}
  *
  * endpoint's job history - GET /v2/api/endpoints/{id}/jobs
  *
  * job info by id         - GET /v2/api/jobs/{id}
  *
  * list workers           - GET /v2/api/workers
  *
  * stop worker            - DELETE /v2/api/workers/{id} (output should be changed)
  *
  */
class HttpApiV2(
  master: MasterService,
  logsMappings: LogStorageMappings) {

  import Directives._
  import HttpApiV2._
  import JsonCodecs._
  import akka.http.scaladsl.model.StatusCodes
  import ParameterDirectives.ParamMagnet
  import akka.http.scaladsl.server._

  private val root = "v2" / "api"
  private val postJobQuery =
    parameters(
      'force ? (false),
      'externalId ?,
      'context ?,
      'mode ?,
      'workerId ?
    ).as(JobRunQueryParams)

  private val jobsQuery = {
    parameters(
      'limit.?(25),
      'offset.?(0)
    ).as(LimitOffsetQuery)
  }

  private val completeOpt = rejectEmptyResponse & complete _

  val route: Route = {
    path( root / "endpoints" ) {
      get { complete {
        master.listEndpoints().map(HttpEndpointInfoV2.convert)
      }}
    } ~
    path( root / "endpoints" / Segment ) { endpointId =>
      get { completeOpt {
        master.endpointInfo(endpointId).map(HttpEndpointInfoV2.convert)
      }}
    } ~
    path( root / "endpoints" / Segment / "jobs" ) { endpointId =>
      get { (jobsQuery & parameter('status * )) { (limits, statuses) =>
        toStatuses(statuses) match {
          case Left(errors) =>
            complete {
              HttpResponse(StatusCodes.BadRequest, entity = errors.mkString(","))
            }
          case Right(statuses) =>
            complete {
              master.endpointHistory(endpointId, limits.limit, limits.offset, statuses)
            }
        }
      }}
    } ~
    path( root / "endpoints" / Segment / "jobs" ) { endpointId =>
      post( postJobQuery { query =>
        entity(as[JobParameters]) { params =>
          if (query.force) {
            completeOpt { runJobForce(endpointId, query, params) }
          } else {
            onComplete(runJob(endpointId, query, params)) {
              case Success(resp) =>
                completeOpt { resp }
              case Failure(ex: IllegalArgumentException) =>
                complete {
                  HttpResponse(StatusCodes.BadRequest, entity = s"Bad Request: ${ex.getMessage}")
                }
              case Failure(ex) =>
                complete {
                  HttpResponse(StatusCodes.InternalServerError, entity = s"Server error: ${ex.getMessage}")
                }
            }
          }
        }
      })
    } ~
    path( root / "jobs" ) {
      get { (jobsQuery & parameter('status * )) { (limits, statuses) =>
        toStatuses(statuses) match {
          case Left(errors) =>
            complete {
              HttpResponse(StatusCodes.BadRequest, entity = errors.mkString(","))
            }
          case Right(statuses) =>
            complete {
              master.getHistory(limits.limit, limits.offset, statuses)
            }
        }
      }}
    } ~
    path( root / "jobs" / Segment ) { jobId =>
      get { completeOpt {
        master.jobStatusById(jobId)
      }}
    } ~
    path( root / "jobs" / Segment / "logs") { jobId =>
      get {
        getFromFile(logsMappings.pathFor(jobId).toFile)
      }
    } ~
    path( root / "jobs" / Segment ) { jobId =>
      delete { completeOpt {
        master.stopJob(jobId)
      }}
    } ~
    path( root / "workers" ) {
      get { complete(master.workers()) }
    } ~
    path( root / "workers" / Segment ) { workerId =>
      delete { completeU(master.stopWorker(workerId).map(_ => ())) }
    } ~
    path ( root / "contexts" ) {
      get { complete(master.contextsSettings.getAll) }
    }
  }


  private def toStatuses(s: Iterable[String]): Either[List[String], List[JobDetails.Status]] = {
    import scala.util._

    val (errors, valid) = s.map(s => Try(JobDetails.Status(s)))
      .foldLeft((List.empty[String], List.empty[JobDetails.Status])) {
        case (acc, Failure(e)) => (acc._1 :+ e.getMessage, acc._2)
        case (acc, Success(status)) => (acc._1, acc._2 :+ status)
    }
    if (errors.nonEmpty) {
      Left(errors)
    } else {
      Right(valid)
    }
  }

  private def runJob(
    endpointId: String,
    queryParams: JobRunQueryParams,
    params: JobParameters): Future[Option[JobStartResponse]] = {

    import cats.data._
    import cats.implicits._

    val out = for {
      ep <- OptionT.fromOption[Future](master.endpointInfo(endpointId))
      request = buildStartRequest(ep.definition.name, queryParams, params)
      resp <- OptionT.liftF(master.runJob(request, Source.Http))
    } yield resp

    out.value
  }

  private def runJobForce(
    endpointId: String,
    queryParams: JobRunQueryParams,
    params: JobParameters
  ): Future[Option[JobResult]] = {

    import cats.data._
    import cats.implicits._
    val out = for {
      ep <- OptionT.fromOption[Future](master.endpointInfo(endpointId))
      request = buildStartRequest(ep.definition.name, queryParams, params)
      resp <- OptionT.liftF(master.forceJobRun(request, Source.Http, Action.Execute))
    } yield resp

    out.value
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
    onSuccess(resource) {
      complete(200, None)
    }
}

object HttpApiV2 {

  case class JobRunQueryParams(
    force: Boolean,
    externalId: Option[String],
    context: Option[String],
    mode: Option[String],
    workerId: Option[String]
  ) {

    def buildRunSettings(): RunSettings = {
      val runMode = mode.flatMap(RunMode.fromString).getOrElse(RunMode.Shared) match {
        case u: RunMode.ExclusiveContext => u.copy(workerId)
        case x => x
      }
      RunSettings(context, runMode)
    }
  }

  case class LimitOffsetQuery(
    limit: Int,
    offset: Int
  )

}
