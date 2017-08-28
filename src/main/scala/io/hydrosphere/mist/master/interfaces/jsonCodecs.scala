package io.hydrosphere.mist.master.interfaces

import java.time.LocalDateTime
import java.time.format.{DateTimeParseException, DateTimeFormatter}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.jobs.JobDetails.{Source, Status}
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobResult}
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError
import spray.json._

import scala.collection.JavaConversions._
import scala.concurrent.duration._

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
      "name", "execute", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

  implicit val httpJobInfoV2F = rootFormat(lazyFormat(jsonFormat(HttpEndpointInfoV2.apply,
    "name", "lang", "execute", "tags", "path", "className", "defaultContext")))

  implicit val workerLinkF = jsonFormat3(WorkerLink)

  implicit val localDateF = new JsonFormat[LocalDateTime] {
    val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

    override def write(o: LocalDateTime): JsValue = {
      val s = formatter.format(o)
      JsString(s)
    }

    override def read(json: JsValue): LocalDateTime = {
      json match {
        case JsString(s) =>
          try { LocalDateTime.parse(s, formatter) }
          catch { case e: DateTimeParseException => throw new DeserializationException(e.getMessage)}
        case x =>
          throw new DeserializationException("LocalDateTime formatter expects json string")
      }
    }
  }

  implicit val mistStatusF = jsonFormat3(MistStatus.apply)

  implicit val jobStartResponseF = jsonFormat1(JobStartResponse)

  implicit val runModeF = new JsonFormat[RunMode] {
    override def write(obj: RunMode): JsValue = {
      obj match {
        case RunMode.Shared => JsObject(("type", JsString("shared")))
        case RunMode.ExclusiveContext(id) =>
          JsObject(
            ("type", JsString("exclusive")),
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
        case "shared" => RunMode.Shared
        case "exclusive" =>
          val id = obj.fields.get("id") match {
            case Some(JsString(i)) => Some(i)
            case _ => None
          }
          RunMode.ExclusiveContext(id)
      }
    }
  }

  implicit val runSettingsF = jsonFormat2(RunSettings.apply)

  implicit val jobStartRequestF = jsonFormat5(EndpointStartRequest)
  implicit val asynJobStartRequestF = jsonFormat4(AsyncEndpointStartRequest)

  implicit val endpointConfigF = jsonFormat4(EndpointConfig.apply)

  implicit val jobResultFormatF = jsonFormat3(JobResult.apply)

  implicit val logEventF = jsonFormat5(LogEvent.apply)

  implicit val devJobStartReqModelF = jsonFormat7(DevJobStartRequestModel.apply)

  implicit val durationF = new JsonFormat[Duration] {

    override def write(obj: Duration): JsValue = {
      obj match {
        case x :FiniteDuration => JsString(s"${x.toSeconds}s")
        case _ => JsString("Inf")
      }
    }

    override def read(json: JsValue): Duration = json match {
      case JsString("Inf") => Duration.Inf
      case JsString(s) =>
        if (s.endsWith("s")) {
          val millis = s.replace("s", "").toLong
          millis.seconds
        } else {
          throw DeserializationException(s"$s should have format [Inf|%d+s] ")
        }
      case x => throw DeserializationException(s"Duration should be JsString")
    }
  }

  implicit val contextConfigF = jsonFormat8(ContextConfig.apply)

  implicit val contextCreateRequestF = jsonFormat8(ContextCreateRequest.apply)

  implicit val updateEventF = new JsonFormat[SystemEvent] {

    implicit val iniF = jsonFormat3(InitializedEvent)
    implicit val queueF = jsonFormat1(QueuedEvent)
    implicit val startedF = jsonFormat2(StartedEvent)
    implicit val canceledF = jsonFormat2(CanceledEvent)
    implicit val finishedF = jsonFormat3(FinishedEvent)
    implicit val failedF = jsonFormat3(FailedEvent)

    implicit val receivedLogF = jsonFormat3(ReceivedLogs)

    override def write(obj: SystemEvent): JsValue = {
      val (name, initial) = obj match {
        case x: InitializedEvent => "initialized" -> x.toJson
        case x: QueuedEvent => "queued" -> x.toJson
        case x: StartedEvent => "started" -> x.toJson
        case x: CanceledEvent => "canceled" -> x.toJson
        case x: FinishedEvent => "finished" -> x.toJson
        case x: FailedEvent => "failed" -> x.toJson
        case x: ReceivedLogs => "logs" -> x.toJson
      }

      val merged = initial.asJsObject.fields + ("event" -> JsString(name))
      JsObject(merged)
    }

    override def read(json: JsValue): SystemEvent = {
      val obj = json.asJsObject
      val name = obj.fields.getOrElse("event", JsString(""))
      name match {
        case JsString("initialized") => obj.convertTo[InitializedEvent]
        case JsString("queued") => obj.convertTo[QueuedEvent]
        case JsString("started") => obj.convertTo[StartedEvent]
        case JsString("finished") => obj.convertTo[FinishedEvent]
        case JsString("failed") => obj.convertTo[FailedEvent]
        case JsString("logs") => obj.convertTo[ReceivedLogs]
        case x => throw new IllegalArgumentException(s"Unknown event type $x")
      }
    }
  }
}

object JsonCodecs extends JsonCodecs

