package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.pattern.{AskTimeoutException, ask}
import akka.stream.scaladsl.Flow
import io.hydrosphere.mist.{Constants, Logger, MistConfig}
import spray.json.{DefaultJsonProtocol, JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, deserializationError, serializationError}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import io.hydrosphere.mist.jobs.{JobConfiguration, JobResult}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport

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
  implicit val jobCreatingRequestFormat = jsonFormat5(JobConfiguration)
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
        entity(as[JobConfiguration]) { jobCreatingRequest =>
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
                complete(HttpResponse(404, entity = "Job route config is not valid. Unknown resource!"))
              case Left(error: ConfigError) =>
                complete(HttpResponse(500, entity = error.reason))
              case Right(jobRequest: JobConfiguration) =>
                doComplete(jobRequest)
            }
          }
        }
      }
    }
  }

  def doComplete(jobRequest: JobConfiguration): akka.http.scaladsl.server.Route =  {

    respondWithHeader(RawHeader("Content-Type", "application/json"))

    complete {
      logger.info(jobRequest.parameters.toString)

      val workerManagerActor = system.actorSelection(s"akka://mist/user/${Constants.Actors.workerManagerName}")

      val future = workerManagerActor.ask(jobRequest)(timeout = MistConfig.Contexts.timeout(jobRequest.name))

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
  }

  def fillJobRequestFromConfig(jobRequestParams: Map[String, Any], jobRoute: String): Either[JobConfigError, JobConfiguration] = {
    try {
      val config = RouteConfig(jobRoute)
      Right(JobConfiguration(config.path, config.className, config.name, jobRequestParams))
    } catch {
      case exc: RouteConfig.RouteNotFoundError => Left(NoRouteError(exc.toString))
      case exc: Throwable => Left(ConfigError(exc.toString))
    }
  }

}
