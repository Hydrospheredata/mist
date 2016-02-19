package com.provectus.lymph.actors

import akka.actor.{Props, ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import akka.pattern.{AskTimeoutException, ask}
import com.provectus.lymph.LymphConfig

import spray.json._
import org.json4s.DefaultFormats
import org.json4s.native.Json

import com.provectus.lymph.jobs.{JobResult, JobConfiguration}

import scala.concurrent.ExecutionContext.Implicits.global

import scala.language.reflectiveCalls

private[lymph] trait HTTPService extends Directives with SprayJsonSupport with DefaultJsonProtocol {

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case number: Int => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case map: Map[String, _] => mapFormat[String, Any] write map
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case unknown => serializationError("Do not understand object of type " + unknown.getClass.getName)
    }
    def read(value: JsValue) = value match {
      case JsNumber(number) => number.toBigInt()
      case JsString(string) => string
      case array: JsArray => listFormat[Any].read(value)
      case jsObject: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }

  implicit val system: ActorSystem
  implicit val materializer: ActorMaterializer

  implicit val jobCreatingRequestFormat = jsonFormat5(JobConfiguration)

  lazy val jobRequestActor:ActorRef = system.actorOf(Props[JobRunner], name = "SyncJobRunner")

  def route : Route = path("jobs") {
    post {
      entity(as[JobConfiguration]) { jobCreatingRequest =>
        complete {

          println(jobCreatingRequest.parameters)

          val future = jobRequestActor.ask(jobCreatingRequest)(timeout = LymphConfig.Contexts.timeout(jobCreatingRequest.name))

          future.recover {
            case e: AskTimeoutException =>
              throw new Exception(e)
          }.map[ToResponseMarshallable] {
            case result: Map[String, Any] =>
              val jobResult = JobResult(success = true, payload = result, request = jobCreatingRequest, errors = List.empty)
              Json(DefaultFormats).write(jobResult)
            case error: String =>
              val jobResult = JobResult(success = false, payload = Map.empty, request = jobCreatingRequest, errors = List(error))
              Json(DefaultFormats).write(jobResult)
            case _ => BadRequest -> "Something went wrong"
          }
        }
      }
    }
  }
}
