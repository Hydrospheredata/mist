package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobResult
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.json.{AnyJsonFormatSupport, JobDetailsJsonSerialization}
import spray.json._

//trait UpdateStatusEventCodec extends DefaultJsonProtocol
//  with AnyJsonFormatSupport
//  with JsonFormat[UpdateStatusEvent] {
//
//  implicit val iniF = jsonFormat2(InitializedEvent)
//  implicit val queueF = jsonFormat1(QueuedEvent)
//  implicit val startedF = jsonFormat2(StartedEvent)
//  implicit val canceledF = jsonFormat2(CanceledEvent)
//  implicit val finishedF = jsonFormat3(FinishedEvent)
//  implicit val failedF = jsonFormat3(FailedEvent)
//
//  override def write(obj: UpdateStatusEvent): JsValue = {
//    val initial = obj.toJson.asJsObject
//
//    val name = obj match {
//      case _: InitializedEvent => "initialized"
//      case _: QueuedEvent => "queued"
//      case _: StartedEvent => "started"
//      case _: CanceledEvent => "canceled"
//      case _: FinishedEvent => "finished"
//      case _: FailedEvent => "failed"
//    }
//    val merged = initial.fields + ("event" -> JsString(name))
//    JsObject(merged)
//  }
//
//  override def read(json: JsValue): UpdateStatusEvent = {
//    val obj = json.asJsObject
//    val name = obj.fields.getOrElse("event", "")
//    name match {
//      case "initialized" => obj.convertTo[InitializedEvent]
//      case "queued" => obj.convertTo[QueuedEvent]
//      case "started" => obj.convertTo[StartedEvent]
//      case "finished" => obj.convertTo[FinishedEvent]
//      case "failed" => obj.convertTo[FailedEvent]
//      case x => throw new IllegalArgumentException(s"Unknown event type $x")
//    }
//  }
//
//}

//object UpdateStatusEventCodec extends UpdateStatusEventCodec

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

  implicit val updateEventF = new JsonFormat[UpdateStatusEvent] {

    implicit val iniF = jsonFormat2(InitializedEvent)
    implicit val queueF = jsonFormat1(QueuedEvent)
    implicit val startedF = jsonFormat2(StartedEvent)
    implicit val canceledF = jsonFormat2(CanceledEvent)
    implicit val finishedF = jsonFormat3(FinishedEvent)
    implicit val failedF = jsonFormat3(FailedEvent)

    override def write(obj: UpdateStatusEvent): JsValue = {
/*      val initial = obj.toJson.asJsObject*/

      val (name, initial) = obj match {
        case x: InitializedEvent => "initialized" -> x.toJson
        case x: QueuedEvent => "queued" -> x.toJson
        case x: StartedEvent => "started" -> x.toJson
        case x: CanceledEvent => "canceled" -> x.toJson
        case x: FinishedEvent => "finished" -> x.toJson
        case x: FailedEvent => "failed" -> x.toJson
      }

      val merged = initial.asJsObject.fields + ("event" -> JsString(name))
      JsObject(merged)
    }

    override def read(json: JsValue): UpdateStatusEvent = {
      val obj = json.asJsObject
      val name = obj.fields.getOrElse("event", JsString(""))
      name match {
        case JsString("initialized") => obj.convertTo[InitializedEvent]
        case JsString("queued") => obj.convertTo[QueuedEvent]
        case JsString("started") => obj.convertTo[StartedEvent]
        case JsString("finished") => obj.convertTo[FinishedEvent]
        case JsString("failed") => obj.convertTo[FailedEvent]
        case x => throw new IllegalArgumentException(s"Unknown event type $x")
      }
    }
  }
}

object JsonCodecs extends JsonCodecs

