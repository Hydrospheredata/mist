package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobResult
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.json.{AnyJsonFormatSupport, JobDetailsJsonSerialization}
import spray.json._

import scala.util.parsing.json.JSONObject

trait UpdateStatusEventCodec extends AnyJsonFormatSupport {

 implicit val printer = CompactPrinter

 def toJson(obj: UpdateStatusEvent): JsValue = {
    obj match {
      case InitializedEvent(id, params) =>
        val externalId = params.externalId.map(JsString(_)).getOrElse(JsNull)
        JsObject(
          ("event", JsString("initialized")),
          ("id", JsString(id)),
          ("externalId", externalId)
        )
      case QueuedEvent(id) =>
        JsObject(
          ("event", JsString("queued")),
          ("id", JsString(id))
        )
      case StartedEvent(id, time) =>
        JsObject(
          ("event", JsString("started")),
          ("id", JsString(id)),
          ("time", JsNumber(time))
        )
      case CanceledEvent(id, time) =>
        JsObject(
          ("event", JsString("canceled")),
          ("id", JsString(id)),
          ("time", JsNumber(time))
        )
      case FinishedEvent(id, time, result) =>
        val jsMap = result.toJson
        JsObject(
          ("event", JsString("finished")),
          ("id", JsString(id)),
          ("time", JsNumber(time)),
          ("result", jsMap)
        )
      case FailedEvent(id, time, error) =>
        JsObject(
          ("event", JsString("failed")),
          ("id", JsString(id)),
          ("time", JsNumber(time)),
          ("error", JsString(error))
        )
    }
  }
}

object UpdateStatusEventCodec extends UpdateStatusEventCodec

trait JsonCodecs extends SprayJsonSupport
  with DefaultJsonProtocol
  with AnyJsonFormatSupport
  with JobDetailsJsonSerialization {

  implicit val printer = CompactPrinter

  implicit val httpJobArgF: RootJsonFormat[HttpJobArg] =
    rootFormat(lazyFormat(jsonFormat(HttpJobArg.apply, "type", "args")))

  implicit val httpJobInfoF = rootFormat(lazyFormat(jsonFormat(HttpJobInfo.apply,
      "name", "execute", "train", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

  implicit val httpJobInfoV2F = rootFormat(lazyFormat(jsonFormat(HttpJobInfoV2.apply,
    "name", "lang", "execute", "train", "serve", "tags")))

  implicit val workerLinkF = jsonFormat2(WorkerLink)

  implicit val jobStartResponseF = jsonFormat1(JobStartResponse)

  implicit val runModeF = new JsonFormat[RunMode] {
    override def write(obj: RunMode): JsValue = {
      obj match {
        case RunMode.Default => JsObject(("type", JsString("default")))
        case RunMode.UniqueContext(id) =>
          JsObject(
            ("type", JsString("uniqueContext")),
            ("id", id.map(JsString(_)).getOrElse(JsNull))
          )
      }
    }

    override def read(json: JsValue): RunMode = {
      val obj = json.asJsObject
      val modeType = obj.fields.get("type") match {
        case Some(JsString(x)) => x
        case _ => throw new IllegalArgumentException(s"Can not extract RunMode from $json")
      }
      modeType match {
        case "default" => RunMode.Default
        case "uniqueContext" =>
          val id = obj.fields.get("id") match {
            case Some(JsString(i)) => Some(i)
            case _ => None
          }
          RunMode.UniqueContext(id)
      }
    }
  }

  implicit val runSettingsF = jsonFormat2(RunSettings.apply)

  implicit val jobStartRequestF = jsonFormat4(JobStartRequest)
  implicit val asynJobStartRequestF = jsonFormat4(AsyncJobStartRequest)

  implicit val jobResultFormatF = jsonFormat4(JobResult.apply)
}

object JsonCodecs extends JsonCodecs

