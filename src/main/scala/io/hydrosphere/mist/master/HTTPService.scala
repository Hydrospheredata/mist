package io.hydrosphere.mist.master

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.pattern.{AskTimeoutException, ask}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobResult}
import io.hydrosphere.mist.{Constants, MistConfig, RouteConfig}
import io.hydrosphere.mist.jobs._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import akka.http.scaladsl.server.{Directive1, Directives}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.util.ConstructFromTuple
import io.hydrosphere.mist.logs.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization

import scala.concurrent.duration.FiniteDuration
import scala.language.reflectiveCalls


/** HTTP interface */
private[mist] trait HTTPService extends Directives with SprayJsonSupport with JobConfigurationJsonSerialization with Logger {

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
    }
  }

  def doComplete(jobRequest: FullJobConfiguration): akka.http.scaladsl.server.Route =  {

    respondWithHeader(RawHeader("Content-Type", "application/json"))

    complete {
      logger.info(jobRequest.parameters.toString)

      val workerManagerActor = system.actorSelection(s"akka://mist/user/${Constants.Actors.workerManagerName}")

      val timeDuration = MistConfig.Contexts.timeout(jobRequest.namespace)
      if(timeDuration.isFinite()) {
        val future = workerManagerActor.ask(jobRequest)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS))

        future
          .recover {
            case _: AskTimeoutException => Right(Constants.Errors.jobTimeOutError)
            case error: Throwable => Right(error.toString)
          }
          .map[ToResponseMarshallable] {
          case result: Either[Map[String, Any], String] =>
            val jobResult: JobResult = result match {
              case Left(jobResults: Map[String, Any]) =>
                JobResult(success = true, payload = jobResults, request = jobRequest, errors = List.empty)
              case Right(error: String) =>
                JobResult(success = false, payload = Map.empty[String, Any], request = jobRequest, errors = List(error))
            }
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), jobResult.toJson.prettyPrint))
        }
      }
      else {
        workerManagerActor ! jobRequest
        JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobRequest, errors = List.empty)
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

}
