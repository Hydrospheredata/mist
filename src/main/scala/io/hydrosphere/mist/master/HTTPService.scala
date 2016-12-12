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
import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobResult, RestificatedJobConfiguration}
import io.hydrosphere.mist.{Constants, Logger, MistConfig, RouteConfig}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json.{DefaultJsonProtocol, JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, deserializationError, serializationError}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.language.reflectiveCalls

private[mist] trait JsonFormatSupport extends DefaultJsonProtocol{
  /** We must implement json parse/serializer for [[Any]] type */
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case number: Int => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case map: Map[String, _] => mapFormat[String, Any] write map
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case unknown: Any => serializationError("Do not understand object of type " + unknown.getClass.getName)
    }
    def read(value: JsValue): Any = value match {
      case JsNumber(number) => number.toBigInt()
      case JsString(string) => string
      case array: JsArray => listFormat[Any].read(value)
      case jsObject: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown: Any => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }

  // JSON to JobConfiguration mapper (6 fields)
  implicit val jobCreatingRequestFormat = jsonFormat5(FullJobConfiguration)
  implicit val jobCreatingRestificatedFormat = jsonFormat3(RestificatedJobConfiguration)
  implicit val jobResultFormat = jsonFormat4(JobResult)

  sealed trait JobConfigError
  case class NoRouteError(reason: String) extends JobConfigError
  case class ConfigError(reason: String) extends JobConfigError
}
/** HTTP interface */
private[mist] trait HTTPService extends Directives with SprayJsonSupport with JsonFormatSupport with Logger{

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer

  // /jobs
  def route: Flow[HttpRequest, HttpResponse, Unit] =  {
    path("jobs") {
      // POST /jobs
      post {
        entity(as[FullJobConfiguration]) { jobCreatingRequest =>
          doComplete(jobCreatingRequest)
        }
      }
    } ~
    pathPrefix("api" / Segment ) { jobRoute =>
      pathEnd {
        post {
          entity(as[Map[String, Any]]) { jobRequestParams =>
            fillJobRequestFromConfig(jobRequestParams, jobRoute) match {
              case Left(error: NoRouteError) =>
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
            case error: AskTimeoutException => Right(Constants.Errors.jobTimeOutError)
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
            HttpResponse(entity = HttpEntity(ContentType(MediaTypes.`application/json`), Json(DefaultFormats).write(jobResult)))
        }
      }
      else {
        workerManagerActor ! jobRequest
        JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobRequest, errors = List.empty)
      }
    }
  }

  def fillJobRequestFromConfig(jobRequestParams: Map[String, Any], jobRoute: String): Either[JobConfigError, FullJobConfiguration] = {
    try {
      val config = RouteConfig(jobRoute)
      Right(FullJobConfiguration(config.path, config.className, config.namespace, jobRequestParams))
    } catch {
      case exc: RouteConfig.RouteNotFoundError => Left(NoRouteError(exc.toString))
      case exc: Throwable => Left(ConfigError(exc.toString))
    }
  }

}
