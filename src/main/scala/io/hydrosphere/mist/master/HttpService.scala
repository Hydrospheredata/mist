package io.hydrosphere.mist.master

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.Directives
import akka.pattern.{AskTimeoutException, ask}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.{Constants, MistConfig, RouteConfig}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import akka.http.scaladsl.server.directives.ParameterDirectives.ParamMagnet

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.language.reflectiveCalls

/** HTTP interface */
private[mist] trait HttpService extends Directives with SprayJsonSupport with JobConfigurationJsonSerialization with Logger {

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer

  // /jobs
  def route: Flow[HttpRequest, HttpResponse, Unit] = {
    path("jobs") {
      // POST /jobs
      post {
        parameters('train.?, 'serve.?) { (train, serve) =>
          val requestBody = train match {
            case Some(_) => as[TrainingJobConfiguration]
            case None => serve match {
              case Some(_) => as[ServingJobConfiguration]
              case None => as[MistJobConfiguration]
            }
          }

          entity(requestBody) { jobCreatingRequest =>
            doComplete(jobCreatingRequest)
          }
        }
      }
    } ~
    pathPrefix("api" / Segment ) { jobRoute =>
      pathEnd {
        post {
          parameters('train.?, 'serve.?) { (train, serve) =>
            entity(as[Map[String, Any]]) { jobRequestParams =>
              // TODO: use FullJobConfigurationBuilder
              fillJobRequestFromConfig(jobRequestParams, jobRoute, train.nonEmpty, serve.nonEmpty) match {
                case Left(_: NoRouteError) =>
                  complete(HttpResponse(StatusCodes.BadRequest, entity = "Job route config is not valid. Unknown resource!"))
                case Left(error: ConfigError) =>
                  complete(HttpResponse(StatusCodes.InternalServerError, entity = error.reason))
                case Right(jobRequest: FullJobConfiguration) =>
                  doComplete(jobRequest)
              }
            }
          }
        }
      }
    } ~
    path("internal" / Segment) { cmd =>
      pathEnd {
        get {
          cmd match {
            case "jobs" => doInternalRequestComplete(ListJobs())
            case "workers" => doInternalRequestComplete(ListWorkers())
            case "routers" => doInternalRequestComplete(ListRouters(extended = true))
          }
        }
      }
    } ~
    pathPrefix("ui") {
      get {
        pathEnd {
          redirect("/ui/", StatusCodes.PermanentRedirect)
        } ~
        pathSingleSlash {
          getFromResource("web/index.html")
        } ~
        getFromResourceDirectory("web")
      }
    } ~
    path("internal" / "jobs" / Segment) { cmd =>
      pathEnd {
        delete {
          doInternalRequestComplete(StopJob(cmd))
        }
      }
    } ~
    path("internal" / "workers" / Segment) { cmd =>
      pathEnd {
        delete {
          doInternalRequestComplete(StopWorker(cmd))
        }
      }
    } ~
    path("internal" / "workers" ) {
      delete {
        doInternalRequestComplete(StopAllWorkers())
      }
    }
  }

  def doComplete(jobRequest: FullJobConfiguration): akka.http.scaladsl.server.Route =  {

    respondWithHeader(RawHeader("Content-Type", "application/json"))

    complete {
      logger.info(jobRequest.parameters.toString)
      
      val distributor = system.actorOf(JobDistributor.props())

      val timeDuration = MistConfig.Contexts.timeout(jobRequest.namespace)
      val jobDetails = JobDetails(jobRequest, JobDetails.Source.Http)
      if(timeDuration.isFinite()) {
        val future = distributor.ask(jobDetails)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS))

        future
          .recover {
            case _: AskTimeoutException => Right(Constants.Errors.jobTimeOutError)
            case error: Throwable => Right(error.toString)
          }
          .map[ToResponseMarshallable] {
          case jobDetails: JobDetails =>
            val jobResult: JobResult = jobDetails.jobResult.getOrElse(Right("Empty result")) match {
              case Left(jobResults: Map[String, Any]) =>
                JobResult(success = true, payload = jobResults, request = jobRequest, errors = List.empty)
              case Right(error: String) =>
                JobResult(success = false, payload = Map.empty[String, Any], request = jobRequest, errors = List(error))
            }
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), Json(DefaultFormats).write(jobResult)))
          case Right(error: String) =>
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), Json(DefaultFormats).write(JobResult(success = false, payload = Map.empty[String, Any], request = jobRequest, errors = List(error)))))
        }
      }
      else {
        distributor ! jobDetails
        val jobResult = JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobRequest, errors = List.empty)
        HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), Json(DefaultFormats).write(jobResult)))
      }
    }
  }

  def fillJobRequestFromConfig(jobRequestParams: Map[String, Any], jobRoute: String, isTraining: Boolean = false, isServing: Boolean = false): Either[JobConfigError, FullJobConfiguration] = {
    try {
      val config = RouteConfig(jobRoute)
      if (isTraining) {
        Right(TrainingJobConfiguration(config.path, config.className, config.namespace, jobRequestParams, None, Some(jobRoute)))
      } else if (isServing) {
        Right(ServingJobConfiguration(config.path, config.className, config.namespace, jobRequestParams, None, Some(jobRoute)))
      } else {
        Right(MistJobConfiguration(config.path, config.className, config.namespace, jobRequestParams, None, Some(jobRoute))) 
      }
    } catch {
      case exc: RouteConfig.RouteNotFoundError => Left(NoRouteError(exc.toString))
      case exc: Throwable => Left(ConfigError(exc.toString))
    }
  }

  def doInternalRequestComplete(cmd: AdminMessage): akka.http.scaladsl.server.Route  = {
    respondWithHeader(RawHeader("Content-Type", "application/json"))
    
    complete {

      val clusterManager = system.actorOf(ClusterManager.props())
      val future = clusterManager.ask(cmd)(timeout = Constants.CLI.timeoutDuration)

      future
        .recover{
          case error: Throwable =>  HttpResponse (entity = HttpEntity (ContentType (MediaTypes.`application/json`), error.toString))
        }
        .map[ToResponseMarshallable] {
          case result: Map[String, Any] =>
            HttpResponse (entity = HttpEntity (ContentType (MediaTypes.`application/json`), Json(DefaultFormats).write(result)))
          case result: List[Any] =>
            HttpResponse (entity = HttpEntity (ContentType (MediaTypes.`application/json`), Json(DefaultFormats).write(result)))
          case result: String =>
            HttpResponse (entity = HttpEntity (ContentType (MediaTypes.`application/json`), Json(DefaultFormats).write(Map("result"->result))))
      }
    }
  }
}
