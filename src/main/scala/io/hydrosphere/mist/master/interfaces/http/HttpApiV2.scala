package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{StatusCodes, StatusCode, HttpResponse}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.directives.ParameterDirectives

import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.master.data.contexts.ContextsStorage
import io.hydrosphere.mist.master.{JobService, MasterService}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{ContextConfig, EndpointConfig, JobStartRequest, RunMode, RunSettings}
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

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

/**
  * Utility for HttpApiv2
  */
object HttpV2Base {

  import Directives._
  import JsonCodecs._
  import akka.http.scaladsl.model.StatusCodes
  import ParameterDirectives.ParamMagnet
  import akka.http.scaladsl.server._

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }

  val root = "v2" / "api"

  val postJobQuery =
    parameters(
      'force ? (false),
      'externalId ?,
      'context ?,
      'mode ?,
      'workerId ?
    ).as(JobRunQueryParams)

  val jobsQuery = {
    parameters(
      'limit.?(25),
      'offset.?(0)
    ).as(LimitOffsetQuery)
  }

  val completeOpt = rejectEmptyResponse & complete _

  def withValidatedStatuses(s: Iterable[String])
      (m: Seq[JobDetails.Status]=> ToResponseMarshallable): StandardRoute = {
    toStatuses(s) match {
      case Left(errors) => complete { HttpResponse(StatusCodes.BadRequest, entity = errors.mkString(",")) }
      case Right(statuses) => complete(m(statuses))
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

  def buildStartRequest(
    routeId: String,
    queryParams: JobRunQueryParams,
    parameters: JobParameters
  ): JobStartRequest = {
    val runSettings = queryParams.buildRunSettings()
    JobStartRequest(routeId, parameters, queryParams.externalId, runSettings)
  }
}

object HttpV2Routes {

  import Directives._
  import JsonCodecs._
  import ParameterDirectives.ParamMagnet
  import akka.http.scaladsl.server._

  import HttpV2Base._

  def workerRoutes(jobService: JobService): Route = {
    path( root / "workers" ) {
      get { complete(jobService.workers()) }
    } ~
    path( root / "workers" / Segment ) { workerId =>
      delete { completeU(jobService.stopWorker(workerId)) }
    }
  }

  def endpointsRoutes(master: MasterService): Route = {
    path( root / "endpoints" ) {
      get { complete {
        master.endpointsInfo.map(HttpEndpointInfoV2.convert)
      }}
    } ~
    path( root / "endpoints" ) {
      post { entity(as[EndpointConfig]) { req =>
        if (master.endpoints.entry(req.name).isDefined) {
          complete(HttpResponse(StatusCodes.Conflict, entity = s"Endpoint with name ${req.name} already exists"))
        } else {
          complete {
            val cfg = master.endpoints.write(req.name, req)
            master.endpointInfo(cfg.name).map(HttpEndpointInfoV2.convert)
          }
        }
      }}
    } ~
    path( root / "endpoints" ) {
      put { entity(as[EndpointConfig]) { req =>
        if (master.endpoints.entry(req.name).isEmpty) {
          complete(HttpResponse(StatusCodes.Conflict, entity = s"Endpoint with name ${req.name} already exists"))
        } else {
          complete {
            val cfg = master.endpoints.write(req.name, req)
            master.endpointInfo(cfg.name).map(HttpEndpointInfoV2.convert)
          }
        }
      }}
    } ~
    path( root / "endpoints" / Segment ) { endpointId =>
      get { completeOpt {
        master.endpointInfo(endpointId).map(HttpEndpointInfoV2.convert)
      }}
    } ~
    path( root / "endpoints" / Segment / "jobs" ) { endpointId =>
      get { (jobsQuery & parameter('status * )) { (limits, rawStatuses) =>
        withValidatedStatuses(rawStatuses) { statuses =>
          master.jobService.endpointHistory(
            endpointId,
            limits.limit, limits.offset, statuses)
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
    }
  }

  def jobsRoutes(master: MasterService): Route = {
    path( root / "jobs" ) {
      get { (jobsQuery & parameter('status * )) { (limits, rawStatuses) =>
        withValidatedStatuses(rawStatuses) { statuses =>
          master.jobService.getHistory(limits.limit, limits.offset, statuses)
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
        getFromFile(master.logStorageMappings.pathFor(jobId).toFile)
      }
    } ~
    path( root / "jobs" / Segment ) { jobId =>
      delete { completeOpt {
        master.jobService.stopJob(jobId)
      }}
    }
  }

  def contextsRoutes(contexts: ContextsStorage): Route = {
    path ( root / "contexts" ) {
      get { complete(contexts.entries) }
    } ~
    path ( root / "contexts" / Segment ) { id =>
      get { complete(contexts.entry(id)) }
    } ~
    path ( root / "contexts" ) {
      post { entity(as[ContextConfig]) { context =>
        if (contexts.entry(context.name).isDefined) {
          complete(HttpResponse(StatusCodes.Conflict, entity = s"Context with name ${context.name} already exists"))
        } else {
          complete { contexts.write(context.name, context) }
        }
      }}
    } ~
    path( root / "contexts" / Segment) { id =>
      put { entity(as[ContextConfig]) { context =>
        if (contexts.entry(context.name).isEmpty) {
          complete(HttpResponse(StatusCodes.NotFound, entity = s"Context with name ${context.name} not found"))
        } else {
          complete { contexts.write(context.name, context) }
        }
      }}
    }
  }

  def apiRoutes(masterService: MasterService): Route = {
    endpointsRoutes(masterService) ~
    jobsRoutes(masterService) ~
    workerRoutes(masterService.jobService) ~
    contextsRoutes(masterService.contexts)
  }

  def apiWithCORS(masterService: MasterService): Route =
    CorsDirective.cors() { apiRoutes(masterService) }
}
