package io.hydrosphere.mist.master.interfaces

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, JobResponse}
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobDetails.{Source, Status}
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobResult}
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.master.interfaces.http.{HttpEndpointInfoV2, HttpJobArg, HttpJobInfo}
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError
import spray.json._

import scala.collection.JavaConversions._

trait AnyJsonFormat extends DefaultJsonProtocol {

  /** We must implement json parse/serializer for [[Any]] type */
  implicit object AnyFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case number: Int => JsNumber(number)
      case number: java.lang.Double => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case javaList: java.util.ArrayList[_] => seqFormat[Any].write(javaList.toList)
      case array: Array[_] => seqFormat[Any].write(array.toList)
      case map: Map[_, _] => mapFormat[String, Any].write(map.asInstanceOf[Map[String, Any]])
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case opt: Option[_] if opt.isDefined => write(opt.get)
      case opt: Option[_] if opt.isEmpty => JsNull
      case unknown: Any => serializationError("Do not understand object of type " + unknown.getClass.getCanonicalName)
    }

    def read(value: JsValue): Any = value match {
      case JsNumber(number) =>
        try {
          number.toIntExact
        } catch {
          case _: ArithmeticException => number.toDouble
        }
      case JsString(string) => string
      case _: JsArray => listFormat[Any].read(value)
      case _: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown: Any => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }
}


trait JobDetailsJsonFormat extends DefaultJsonProtocol with AnyJsonFormat {

  implicit object EitherJobResultSupport extends RootJsonFormat[JobResponseOrError] {
    override def write(obj: JobResponseOrError): JsValue = obj match {
      case Right(s: String) => JsString(s)
      case Left(m) => mapFormat[String, Any].write(m)
    }

    override def read(json: JsValue): JobResponseOrError = json match {
      case JsString(str) => Right(str)
      case _: JsObject => Left(mapFormat[String, Any].read(json))
      case _ => throw DeserializationException("Either[Map, String] can be only string or object")
    }
  }


  implicit object JobStatusSupport extends RootJsonFormat[JobDetails.Status] {
    override def write(obj: Status): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Status = json match {
      case JsString(str) => JobDetails.Status(str)
      case _ => throw DeserializationException("JobDetails.Status must be a string")
    }
  }

  implicit object JobSourceSupport extends RootJsonFormat[JobDetails.Source] {
    override def write(obj: Source): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Source = json match {
      case JsString(str) => JobDetails.Source(str)
      case _ => throw DeserializationException("JobDetails.Source must be a string")
    }
  }

  implicit object ActionSupport extends RootJsonFormat[Action] {
    override def write(obj: Action): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Action = json match {
      case JsString(str) => Action(str)
      case _ => throw DeserializationException("JobConfiguration.Action must be a string")
    }
  }

  implicit val jobParamsF = jsonFormat4(JobParams.apply)
  implicit val jobDetailsJsonFormat: RootJsonFormat[JobDetails] = jsonFormat12(JobDetails.apply)

}


trait JsonCodecs extends SprayJsonSupport
  with DefaultJsonProtocol
  with AnyJsonFormat
  with JobDetailsJsonFormat {

  implicit val printer = CompactPrinter

  implicit val httpJobArgF: RootJsonFormat[HttpJobArg] =
    rootFormat(lazyFormat(jsonFormat(HttpJobArg.apply, "type", "args")))

  implicit val httpJobInfoF = rootFormat(lazyFormat(jsonFormat(HttpJobInfo.apply,
      "name", "execute", "train", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

  implicit val httpJobInfoV2F = rootFormat(lazyFormat(jsonFormat(HttpEndpointInfoV2.apply,
    "name", "lang", "execute", "train", "serve", "tags")))

  implicit val workerLinkF = jsonFormat2(WorkerLink)

  implicit val jobStartResponseF = jsonFormat1(JobStartResponse)

  implicit val runModeF = new JsonFormat[RunMode] {
    override def write(obj: RunMode): JsValue = {
      obj match {
        case RunMode.Default => JsObject(("type", JsString("default")))
        case RunMode.ExclusiveContext(id) =>
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
          RunMode.ExclusiveContext(id)
      }
    }
  }

  implicit val runSettingsF = jsonFormat2(RunSettings.apply)

  implicit val jobStartRequestF = jsonFormat4(JobStartRequest)
  implicit val asynJobStartRequestF = jsonFormat4(AsyncJobStartRequest)

  implicit val jobResultFormatF = jsonFormat4(JobResult.apply)

  implicit val updateEventF = new JsonFormat[UpdateStatusEvent] {

    implicit val iniF = jsonFormat2(InitializedEvent)
    implicit val queueF = jsonFormat2(QueuedEvent)
    implicit val startedF = jsonFormat2(StartedEvent)
    implicit val canceledF = jsonFormat2(CanceledEvent)
    implicit val finishedF = jsonFormat3(FinishedEvent)
    implicit val failedF = jsonFormat3(FailedEvent)

    override def write(obj: UpdateStatusEvent): JsValue = {
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

