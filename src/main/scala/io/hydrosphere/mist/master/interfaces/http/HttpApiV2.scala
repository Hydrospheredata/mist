package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.directives.ParameterDirectives
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobResult}
import io.hydrosphere.mist.master.{JobService, MasterService}
import io.hydrosphere.mist.master.data.endpoints.EndpointsStorage
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models.{ContextConfig, EndpointConfig, JobStartRequest, JobStartResponse, RunMode, RunSettings}
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps


class HttpApiV2(
  master: MasterService,
  logsMappings: LogStorageMappings,
  endpoints: EndpointsStorage
) {

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
        master.endpointsInfo.map(HttpEndpointInfoV2.convert)
      }}
    } ~
    path( root / "endpoints" ) {
      post { entity(as[EndpointConfig]) { req =>
        complete { master.endpoints.write(req.name, req) }
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
              master.jobService.endpointHistory(endpointId, limits.limit, limits.offset, statuses)
            }
        }
      }}
    } ~
    path( root / "endpoints" / Segment / "jobs" ) { endpointId =>
      post( postJobQuery { query =>
        entity(as[JobParameters]) { params =>
          val jobReq = buildStartRequest(endpointId, query, params)
          if (query.force) {
            completeOpt { master.forceJobRun(jobReq, Source.Http) }
          } else {
            completeOpt { master.runJob(jobReq, Source.Http) }
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
              master.jobService.getHistory(limits.limit, limits.offset, statuses)
            }
        }
      }}
    } ~
    path( root / "jobs" / Segment ) { jobId =>
      get { completeOpt {
        master.jobService.jobStatusById(jobId)
      }}
    } ~
    path( root / "jobs" / Segment / "logs") { jobId =>
      get {
        getFromFile(logsMappings.pathFor(jobId).toFile)
      }
    } ~
    path( root / "jobs" / Segment ) { jobId =>
      delete { completeOpt {
        master.jobService.stopJob(jobId)
      }}
    } ~
    path( root / "workers" ) {
      get { complete(master.jobService.workers()) }
    } ~
    path( root / "workers" / Segment ) { workerId =>
      delete { completeU(master.jobService.stopWorker(workerId)) }
    } ~
    path ( root / "contexts" ) {
      get { complete(master.contexts.entries) }
    } ~
    path ( root / "contexts" / Segment ) { id =>
      get { complete(master.contexts.entry(id)) }
    } ~
    path ( root / "contexts" ) {
      post { entity(as[ContextConfig]) { context =>
        complete { master.contexts.write(context.name, context) }
      }}
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
