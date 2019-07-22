package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.Http
import java.io.{PrintWriter, StringWriter, File}

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

import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import io.hydrosphere.mist.BuildInfo
import io.hydrosphere.mist.master.execution.ExecutionService
import io.hydrosphere.mist.master.jobs.FunctionsService
import io.hydrosphere.mist.utils.Logger
import mist.api.data.JsMap

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util._
import cats.data.NonEmptyList

case class JobRunQueryParams(
  force: Boolean,
  externalId: Option[String],
  context: Option[String],
  startTimeout: Duration,
  performTimeout: Duration
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

  val postJobQuery: Directive1[JobRunQueryParams] = {
    def mkDuration(s: Symbol): Directive1[Duration] = parameter(s ?).map {
      case Some(s) => Duration.create(s)
      case None => Duration.Inf
    }
    val all = parameters(
      'force ? (false),
      'externalId ?,
      'context ?
    ) & mkDuration('startTimeout) & mkDuration('performTimeout)

    all.as(JobRunQueryParams)
  }

  val paginationQuery = {
    parameters(
      'limit.?(25),
      'offset.?(0),
      'paginate.?(false)
    ).as(PaginationQuery)
  }

  def completeOpt(m: => ToResponseMarshallable): Route = rejectEmptyResponse(complete(m))

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

  val statusesQuery: Directive1[Option[NonEmptyList[JobDetails.Status]]] = {
    parameter('status *).flatMap(raw => {
      toStatuses(raw) match {
        case Left(errors) => reject(ValidationRejection(s"Unknown statuses: ${errors.mkString(",")}"))
        case Right(validated) => provide(NonEmptyList.fromList(validated))
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
    FunctionStartRequest(
      routeId, parameters, queryParams.externalId, runSettings,
      timeouts = Timeouts(queryParams.startTimeout, queryParams.performTimeout)
    )
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
      get { (paginationQuery & statusesQuery) { (pagination, maybeStatuses) =>
        val req = JobDetailsRequest(pagination.limit, pagination.offset)
          .withOptFilter(maybeStatuses.map(FilterClause.ByStatuses))
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

  def functionsCrud(functions: FunctionsService): Route = {
    path( root / "functions" ) {
      get { complete {
        functions.allFunctions.map(_.map(HttpFunctionInfoV2.convert))
      }}
    } ~
    path( root / "functions" ) {
      post(parameter('force? false) { force =>
        entity(as[FunctionConfig]) { req =>
          if (force) {
            completeF(functions.updateConfig(req), StatusCodes.BadRequest)
          } else {
            val rsp =
              functions.hasFunction(req.name).ifM[HttpFunctionInfoV2](
                Future.failed(new IllegalStateException(s"Endpoint ${req.name} already exists")),
                functions.update(req).map(HttpFunctionInfoV2.convert)
              )

            completeF(rsp, StatusCodes.BadRequest)
          }
        }
      })
    } ~
    path( root / "functions" ) {
      put { entity(as[FunctionConfig]) { req =>
        val rsp = functions.hasFunction(req.name).ifM(
          functions.update(req).map(HttpFunctionInfoV2.convert),
          Future.failed(new IllegalStateException(s"Endpoint ${req.name} already exists"))
        )
        completeF(rsp, StatusCodes.BadRequest)
      }}
    } ~
    path( root / "functions" / Segment ) { functionId =>
      get { completeOpt {
        functions.getFunctionInfo(functionId).map(_.map(HttpFunctionInfoV2.convert))
      }} ~
        delete {
          completeOpt {
            functions.delete(functionId).map(_.map(HttpFunctionInfoV2.convert))
          }
        }
    }
  }

  def functionsJobs(main: MainService): Route = {
    path( root / "functions" / Segment / "jobs" ) { functionId =>
      get { (paginationQuery & statusesQuery) { (pagination, maybeStatuses) =>
        val req = JobDetailsRequest(pagination.limit, pagination.offset)
          .withOptFilter(maybeStatuses.map(FilterClause.ByStatuses))
          .withFilter(FilterClause.ByFunctionId(functionId))

        onSuccess(main.execution.getHistory(req))(rsp => {
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
            completeOpt { main.forceJobRun(jobReq, JobDetails.Source.Http) }
          } else {
            completeOpt { main.runJob(jobReq, JobDetails.Source.Http) }
          }
        }
      })
    }
  }

  def functionAllRoutes(main: MainService): Route = {
    functionsCrud(main.functionInfoService) ~ functionsJobs(main)
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
    path(root / "artifacts" / Segment) { filename =>
      delete {
        artifactRepo.delete(filename) match {
          case Some(_) => complete(HttpResponse(StatusCodes.OK, entity = filename))
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
        get { (paginationQuery & statusesQuery) { (pagination, maybeStatuses) =>
          val req = JobDetailsRequest(pagination.limit, pagination.offset)
            .withOptFilter(maybeStatuses.map(FilterClause.ByStatuses))

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
      path( Segment ) { jobId =>
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
    path (root / "contexts" / Segment) { id =>
      delete { completeOpt(ctxCrud.delete(id)) }
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

  def apiRoutes(main: MainService, artifacts: ArtifactRepository, mistHome: String): Route = {
    val exceptionHandler = ExceptionHandler({ case e => {
      val (status, msg) = e match {
        case ex @ (_: IllegalArgumentException  | _: IllegalStateException) =>
          (StatusCodes.BadRequest, "Bad request")
        case ex =>
          (StatusCodes.InternalServerError, "Server error")
      }
      val errorMsg = {
        val writer = new StringWriter()
        e.printStackTrace(new PrintWriter(writer))
        writer.toString
      }
      val fullMsg = msg + ":\n" + errorMsg
      complete(HttpResponse(status, entity = fullMsg))
    }})

    handleExceptions(exceptionHandler) {
      functionAllRoutes(main) ~
      jobsRoutes(main) ~
      workerRoutes(main.execution) ~
      contextsRoutes(main) ~
      internalArtifacts(mistHome) ~
      artifactRoutes(artifacts) ~
      statusApi ~
      rootEndpoint
    }
  }

  def apiWithCORS(masterService: MainService, artifacts: ArtifactRepository, mistHome: String): Route =
    CorsDirective.cors() { apiRoutes(masterService, artifacts, mistHome) }
}
