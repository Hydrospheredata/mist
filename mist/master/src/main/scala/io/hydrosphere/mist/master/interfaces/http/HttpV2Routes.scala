package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.Http
import java.io.File

import akka.http.scaladsl.marshalling.{ToEntityMarshaller, ToResponseMarshallable}
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.directives.{FileInfo, ParameterDirectives}
import akka.stream.scaladsl
import cats.data.OptionT
import cats.implicits._
import io.hydrosphere.mist.utils.FutureOps._
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models._
import org.apache.commons.codec.digest.DigestUtils
import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.BuildInfo
import io.hydrosphere.mist.master.execution.ExecutionService
import io.hydrosphere.mist.utils.Logger
import mist.api.data.JsMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util._

case class JobRunQueryParams(
  force: Boolean,
  externalId: Option[String],
  context: Option[String]
) {

  def buildRunSettings(): RunSettings = {
    RunSettings(context)
  }
}

case class PaginationQuery(
  limit: Int,
  offset: Int,
  paginate: Boolean
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
      'context ?
    ).as(JobRunQueryParams)

  val paginationQuery = {
    parameters(
      'limit.?(25),
      'offset.?(0),
      'paginate.?(false)
    ).as(PaginationQuery)
  }

  val completeOpt = rejectEmptyResponse & complete _

  def completeTry[A : ToEntityMarshaller](f: => Try[A], errorCode: StatusCode): StandardRoute = {
    f match {
      case Success(a) => complete(a)
      case Failure(e) => complete(HttpResponse(errorCode, entity = s"${e.getMessage}"))
    }
  }

  def completeF[A: ToEntityMarshaller](f: => Future[A], errorCode: StatusCode): Route = {
    onComplete(f) {
      case Success(a) => complete(a)
      case Failure(ex) => complete(HttpResponse(errorCode, entity=s"${ex.getClass}: ${ex.getMessage}"))
    }
  }
  def completeOptF[A: ToEntityMarshaller](f: => Future[Option[A]], errorCode: StatusCode): Route = {
    onComplete(f) {
      case Success(Some(a)) => complete(a)
      case Success(None)    => complete(HttpResponse(errorCode, entity="Not found"))
      case Failure(ex)      => complete(HttpResponse(errorCode, entity=s"${ex.getMessage}"))
    }
  }

  def withValidatedStatuses(s: Iterable[String])
      (m: Seq[JobDetails.Status]=> ToResponseMarshallable): StandardRoute = {
    toStatuses(s) match {
      case Left(errors) => complete { HttpResponse(StatusCodes.BadRequest, entity = errors.mkString(",")) }
      case Right(statuses) => complete(m(statuses))
    }
  }

  val statusesQuery: Directive1[Seq[JobDetails.Status]] = {
    parameter('status *).flatMap(raw => {
      toStatuses(raw) match {
        case Left(errors) => reject(ValidationRejection(s"Unknown statuses: ${errors.mkString(",")}"))
        case Right(validated) => provide(validated)
      }
    })
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
    parameters: JsMap
  ): FunctionStartRequest = {
    val runSettings = queryParams.buildRunSettings()
    FunctionStartRequest(routeId, parameters, queryParams.externalId, runSettings)
  }
}

object HttpV2Routes extends Logger {

  import Directives._
  import JsonCodecs._
  import ParameterDirectives.ParamMagnet
  import akka.http.scaladsl.server._

  import HttpV2Base._

  def workerRoutes(jobService: ExecutionService): Route = {
    path( root / "workers" ) {
      get { complete(jobService.workers()) }
    } ~
    path( root / "workers" / Segment ) { workerId =>
      delete {
        completeU(jobService.stopWorker(workerId))
      } ~
      get {
        completeOpt { jobService.getWorkerLink(workerId) }
      }
    } ~
    path( root / "workers"/ Segment / "jobs") { workerId =>
      get { (paginationQuery & statusesQuery) { (pagination, statuses) =>
        val req = JobDetailsRequest(pagination.limit, pagination.offset)
          .withFilter(FilterClause.ByStatuses(statuses))
          .withFilter(FilterClause.ByWorkerId(workerId))

        onSuccess(jobService.getHistory(req))(rsp => {
          if (pagination.paginate)
            complete(rsp)
          else
            complete(rsp.jobs)
        })
      }
    }}
  }

  def functionRoutes(master: MainService): Route = {
    path( root / "functions" ) {
      get { complete {
        master.functionInfoService.allFunctions.map(_.map(HttpFunctionInfoV2.convert))
      }}
    } ~
    path( root / "functions" ) {
      post(parameter('force? false) { force =>
        entity(as[FunctionConfig]) { req =>
          if (force) {
            completeF(master.functions.update(req), StatusCodes.BadRequest)
          } else {
            val rsp = master.functions.get(req.name).flatMap({
              case Some(ep) =>
                val e = new IllegalStateException(s"Endpoint ${ep.name} already exists")
                Future.failed(e)
              case None =>
                for {
                  fullInfo     <- master.functionInfoService.getFunctionInfoByConfig(req)
                  updated      <- master.functions.update(req)
                  functionInfo =  HttpFunctionInfoV2.convert(fullInfo)
                } yield functionInfo
            })

            completeF(rsp, StatusCodes.BadRequest)
          }
        }
      })
    } ~
    path( root / "functions" ) {
      put { entity(as[FunctionConfig]) { req =>

        onSuccess(master.functions.get(req.name)) {
          case None =>
            val resp = HttpResponse(StatusCodes.Conflict, entity = s"Endpoint with name ${req.name} not found")
            complete(resp)
          case Some(_) =>
            val res = for {
              updated      <- master.functions.update(req)
              fullInfo     <- master.functionInfoService.getFunctionInfoByConfig(updated)
              functionInfo =  HttpFunctionInfoV2.convert(fullInfo)
            } yield functionInfo

            completeF(res, StatusCodes.BadRequest)
        }
      }}
    } ~
    path( root / "functions" / Segment ) { functionId =>
      get { completeOpt {
        master.functionInfoService
          .getFunctionInfo(functionId)
          .map(_.map(HttpFunctionInfoV2.convert))
      }}
    } ~
    path( root / "functions" / Segment / "jobs" ) { functionId =>
      get { (paginationQuery & statusesQuery) { (pagination, statuses) =>
        val req = JobDetailsRequest(pagination.limit, pagination.offset)
          .withFilter(FilterClause.ByStatuses(statuses))
          .withFilter(FilterClause.ByFunctionId(functionId))

        onSuccess(master.execution.getHistory(req))(rsp => {
          if (pagination.paginate)
            complete(rsp)
          else
            complete(rsp.jobs)
        })
      }}
    } ~
    path( root / "functions" / Segment / "jobs" ) { functionId =>
      post( postJobQuery { query =>
        entity(as[JsMap]) { params =>
          val jobReq = buildStartRequest(functionId, query, params)
          if (query.force) {
            completeOpt { master.forceJobRun(jobReq, JobDetails.Source.Http) }
          } else {
            completeOpt { master.runJob(jobReq, JobDetails.Source.Http) }
          }
        }
      })
    }
  }

  def artifactRoutes(artifactRepo: ArtifactRepository): Route = {
    path(root / "artifacts") {
      get {
        complete {
          artifactRepo.listPaths()
        }
      }
    } ~
    path(root / "artifacts" / Segment) { filename =>
      get {
        artifactRepo.get(filename) match {
          case Some(file) => getFromFile(file)
          case None => complete {
            HttpResponse(StatusCodes.NotFound, entity = s"No file found by name $filename")
          }
        }
      }
    } ~
    path(root / "artifacts" / Segment / "sha" ) { filename =>
      get {
        artifactRepo.get(filename) match {
          case Some(file) => complete {
            DigestUtils.sha1Hex(Files.newInputStream(file.toPath))
          }
          case None => complete {
            HttpResponse(StatusCodes.NotFound, entity = s"No file found by name $filename")
          }
        }
      }
    } ~
    path(root / "artifacts") {
      post { parameters('force? false) { force => {
        def storeArtifactFile(metadata: FileInfo, tempFile: File): Route = {
          onSuccess(artifactRepo.store(tempFile, metadata.fileName)) { f =>
            tempFile.delete
            complete { HttpResponse(StatusCodes.OK, entity = s"${f.getName}") }
          }
        }
        uploadedFile("file") {
          case (metadata, tempFile) =>
            if (force) {
              storeArtifactFile(metadata, tempFile)
            } else {
              artifactRepo.get(metadata.fileName) match {
                case Some(_) => complete {
                  HttpResponse(
                    StatusCodes.Conflict,
                    entity = s"Filename must be unique: found ${metadata.fileName} in repository"
                  )
                }
                case None =>
                  storeArtifactFile(metadata, tempFile)
              }
            }
        }
      }
    }}}
  }

  def internalArtifacts(mistHome: String): Route = {
    path(root / "artifacts_internal" /  "mist-worker.jar" ) {
      get {
        val path = Paths.get(mistHome, "mist-worker.jar")
        getFromFile(path.toString)
      }
    }
  }

  def jobsRoutes(master: MainService): Route = {
    pathPrefix( root / "jobs") {
      pathEnd {
        get { (paginationQuery & statusesQuery) { (pagination, statuses) =>
          val req = JobDetailsRequest(pagination.limit, pagination.offset)
            .withFilter(FilterClause.ByStatuses(statuses))

          onSuccess(master.execution.getHistory(req))(rsp => {
            if (pagination.paginate)
              complete(rsp)
            else
              complete(rsp.jobs)
          })
        }}
      } ~
      path( Segment ) { jobId =>
        get {
          completeOpt(master.execution.jobStatusById(jobId))
        }
      } ~
      path( root / "jobs" / Segment ) { jobId =>
        delete { completeOpt {
          master.execution.stopJob(jobId)
        }}
      } ~
      path( Segment / "logs") { jobId =>
        get {
          onSuccess(master.execution.jobStatusById(jobId)) {
            case Some(_) =>
              master.logsPaths.pathFor(jobId).toFile match {
                case file if file.exists => getFromFile(file)
                case _ => complete { HttpResponse(StatusCodes.OK, entity=HttpEntity.Empty) }
              }
            case None =>
              complete { HttpResponse(StatusCodes.NotFound, entity=s"Job $jobId not found")}
          }
        }
      }
    }
  }

  def contextsRoutes(ctxCrud: ContextsCRUDLike): Route = {
    path ( root / "contexts" ) {
      get { complete(ctxCrud.getAll()) }
    } ~
    path ( root / "contexts" / Segment ) { id =>
      get { completeOpt(ctxCrud.get(id)) }
    } ~
    path ( root / "contexts" ) {
      post { entity(as[ContextCreateRequest]) { req =>
        complete(ctxCrud.create(req))
      }}
    } ~
    path( root / "contexts" / Segment) { id =>
      put { entity(as[ContextConfig]) { context =>
        onSuccess(ctxCrud.get(id)) {
          case Some(_) => complete { ctxCrud.update(context) }
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
        complete(MistStatus.create)
      }
    }
  }

  val rootEndpoint: Route = {
    val started = MistStatus.Started
    val version = BuildInfo.version
    val pageData =
      s"""<p>
         |  Mist version: $version, started: $started,
         |  <br/>
         |  <a href="/ui/">ui link</a>
         |</p>
       """.stripMargin
    pathSingleSlash {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, pageData))
    }
  }

  def apiRoutes(masterService: MainService, artifacts: ArtifactRepository, mistHome: String): Route = {
    val exceptionHandler =
      ExceptionHandler {
        case ex @ (_: IllegalArgumentException  | _: IllegalStateException) =>
          complete((StatusCodes.BadRequest, s"Bad request: ${ex.getMessage}"))
        case ex =>
          complete(HttpResponse(StatusCodes.InternalServerError, entity = s"Server error: ${ex.getMessage}"))
      }
    handleExceptions(exceptionHandler) {
      functionRoutes(masterService) ~
      jobsRoutes(masterService) ~
      workerRoutes(masterService.execution) ~
      contextsRoutes(masterService) ~
      internalArtifacts(mistHome) ~
      artifactRoutes(artifacts) ~
      statusApi ~
      rootEndpoint
    }
  }

  def apiWithCORS(masterService: MainService, artifacts: ArtifactRepository, mistHome: String): Route =
    CorsDirective.cors() { apiRoutes(masterService, artifacts, mistHome) }
}
