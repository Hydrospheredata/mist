package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.{ToEntityMarshaller, ToResponseMarshallable}
import akka.http.scaladsl.model.{HttpEntity, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.directives.ParameterDirectives
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master.{JobService, MasterService}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.TypeAlias.JobParameters

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util._

case class JobRunQueryParams(
  force: Boolean,
  externalId: Option[String],
  context: Option[String],
  workerId: Option[String]
) {

  def buildRunSettings(): RunSettings = {
    RunSettings(context, workerId)
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
  import ParameterDirectives.ParamMagnet
  import akka.http.scaladsl.model.StatusCodes
  import akka.http.scaladsl.server._

  def completeU(resource: Future[Unit]): Route =
    onSuccess(resource) { complete(200, None) }

  val root = "v2" / "api"

  val postJobQuery =
    parameters(
      'force ? (false),
      'externalId ?,
      'context ?,
      'workerId ?
    ).as(JobRunQueryParams)

  val jobsQuery = {
    parameters(
      'limit.?(25),
      'offset.?(0)
    ).as(LimitOffsetQuery)
  }

  val completeOpt = rejectEmptyResponse & complete _

  def completeTry[A : ToEntityMarshaller](f: => Try[A], errorCode: StatusCode): StandardRoute = {
    f match {
      case Success(a) => complete(a)
      case Failure(e) => complete(HttpResponse(errorCode, entity = e.getMessage))
    }
  }

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
  ): EndpointStartRequest = {
    val runSettings = queryParams.buildRunSettings()
    EndpointStartRequest(routeId, parameters, queryParams.externalId, runSettings)
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
    val exceptionHandler =
      ExceptionHandler {
        case iae: IllegalArgumentException =>
          complete((StatusCodes.BadRequest, s"Bad request: ${iae.getMessage}"))
        case ex =>
          complete(HttpResponse(StatusCodes.InternalServerError, entity = s"Server error: ${ex.getMessage}"))
      }

    path( root / "endpoints" ) {
      get { complete {
        master.endpointsInfo.map(_.map(HttpEndpointInfoV2.convert))
      }}
    } ~
    path( root / "endpoints" ) {
      post { entity(as[EndpointConfig]) { req =>
        onSuccess(master.endpoints.get(req.name)) {
          case Some(_) =>
            val resp = HttpResponse(StatusCodes.Conflict, entity = s"Endpoint with name ${req.name} already exists")
            complete(resp)
          case None =>
            completeTry({
              master.loadEndpointInfo(req).map(fullInfo => {
                master.endpoints.update(req)
                fullInfo
              }).map(HttpEndpointInfoV2.convert)
            }, StatusCodes.BadRequest)
        }
      }}
    } ~
    path( root / "endpoints" ) {
      put { entity(as[EndpointConfig]) { req =>

        onSuccess(master.endpoints.get(req.name)) {
          case None =>
            val resp = HttpResponse(StatusCodes.Conflict, entity = s"Endpoint with name ${req.name} already exists")
            complete(resp)
          case Some(_) =>
            completeTry({
              master.loadEndpointInfo(req).map(fullInfo => {
                master.endpoints.update(req)
                fullInfo
              }).map(HttpEndpointInfoV2.convert)
            }, StatusCodes.BadRequest)
        }
      }}
    } ~
    path( root / "endpoints" / Segment ) { endpointId =>
      get { completeOpt {
        master.endpointInfo(endpointId).map(_.map(HttpEndpointInfoV2.convert))
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
          handleExceptions(exceptionHandler) {
            val jobReq = buildStartRequest(endpointId, query, params)
            if (query.force) {
              completeOpt { master.forceJobRun(jobReq, Source.Http) }
            } else {
              completeOpt { master.runJob(jobReq, Source.Http) }
            }
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
        onSuccess(master.jobService.jobStatusById(jobId)) {
          case Some(_) =>
            master.logStorageMappings.pathFor(jobId).toFile match {
              case file if file.exists => getFromFile(file)
              case _ => complete { HttpResponse(StatusCodes.OK, entity=HttpEntity.Empty) }
            }
          case None =>
            complete { HttpResponse(StatusCodes.NotFound, entity=s"Job $jobId not found")}
        }
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
      get { complete(contexts.all) }
    } ~
    path ( root / "contexts" / Segment ) { id =>
      get { completeOpt(contexts.get(id)) }
    } ~
    path ( root / "contexts" ) {
      post { entity(as[ContextCreateRequest]) { context =>
        onSuccess(contexts.get(context.name)) {
          case None => complete {
            val config = context.toContextWithFallback(contexts.defaultConfig)
            contexts.update(config)
          }
          case Some(_) =>
            val rsp = HttpResponse(StatusCodes.Conflict, entity = s"Context with name ${context.name} already exists")
            complete(rsp)
        }
      }}
    } ~
    path( root / "contexts" / Segment) { id =>
      put { entity(as[ContextConfig]) { context =>
        onSuccess(contexts.get(context.name)) {
          case Some(_) => complete { contexts.update(context) }
          case None =>
            val rsp = HttpResponse(StatusCodes.NotFound, entity = s"Context with name ${context.name} not found")
            complete(rsp)
        }
      }}
    }
  }

  def statusApi: Route = {
    path( root / "status" ) {
      get {
        complete(MistStatus.Value)
      }
    }
  }

  def apiRoutes(masterService: MasterService): Route = {
    endpointsRoutes(masterService) ~
    jobsRoutes(masterService) ~
    workerRoutes(masterService.jobService) ~
    contextsRoutes(masterService.contexts) ~
    statusApi
  }

  def apiWithCORS(masterService: MasterService): Route =
    CorsDirective.cors() { apiRoutes(masterService) }
}
