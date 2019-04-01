package io.hydrosphere.mist.master.interfaces

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import io.hydrosphere.mist.core.CommonData.{Action, JobParams, WorkerInitInfo}
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.execution.WorkerLink
import io.hydrosphere.mist.master.interfaces.http._
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.{JobDetails, JobDetailsResponse, JobResult}
import mist.api.{data => mdata}
import spray.json._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Try

trait AnyJsonFormat extends DefaultJsonProtocol {

  /** We must implement json parse/serializer for [[Any]] type */
  implicit object AnyFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case number: Int => JsNumber(number)
      case number: java.lang.Double => JsNumber(number)
      case number: java.lang.Long => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case javaList: java.util.ArrayList[_] => seqFormat[Any].write(javaList.asScala.toList)
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
        Try(number.toIntExact)
          .orElse(Try(number.toLongExact))
          .getOrElse(number.toDouble)
      case JsString(string) => string
      case _: JsArray => listFormat[Any].read(value)
      case _: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown: Any => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }
}

trait MDataFormat {

  implicit val mDataFormat = new RootJsonFormat[mdata.JsData] {
    override def read(json: JsValue): mdata.JsData = {
      json match {
        case JsObject(fields) => mdata.JsMap(fields.mapValues(v => read(v)))
        case JsString(s) => mdata.JsString(s)
        case JsNumber(d) => mdata.JsNumber(d)
        case JsFalse => mdata.JsFalse //TODO MBoolean can has default false and true
        case JsTrue  => mdata.JsTrue
        case JsNull  => mdata.JsNull
        case JsArray(values) => mdata.JsList(values.map(read))
      }
    }

    override def write(obj: mdata.JsData): JsValue = {
      obj match {
        case mdata.JsNumber(d) => JsNumber(d)
        case mdata.JsTrue      => JsTrue
        case mdata.JsFalse     => JsFalse
        case mdata.JsString(s) => JsString(s)
        case mdata.JsNull      => JsNull
        case mdata.JsUnit      => JsObject(Map.empty[String, JsValue])
        case mdata.JsList(values) => JsArray(values.map(v => write(v)).toVector)
        case mdata.JsMap(map)     => JsObject(map.mapValues(v => write(v)))
      }
    }
  }

  implicit val jsLikeMapFormat = new RootJsonFormat[mdata.JsMap] {
    override def write(obj: mdata.JsMap): JsValue = JsObject(obj.map.mapValues(v => mDataFormat.write(v)))
    override def read(json: JsValue): mdata.JsMap = json match {
      case JsObject(fields) => mdata.JsMap(fields.mapValues(v => mDataFormat.read(v)))
      case other => throw new DeserializationException(s"Json object required: got $other")
    }
  }
}


trait JobDetailsJsonFormat extends DefaultJsonProtocol with AnyJsonFormat with MDataFormat {

  implicit object JobStatusSupport extends RootJsonFormat[JobDetails.Status] {
    override def write(obj: JobDetails.Status): JsValue = JsString(obj.toString)

    override def read(json: JsValue): JobDetails.Status = json match {
      case JsString(str) => JobDetails.Status(str)
      case _ => throw DeserializationException("JobDetails.Status must be a string")
    }
  }

  implicit object JobSourceSupport extends RootJsonFormat[JobDetails.Source] {
    override def write(obj: JobDetails.Source): JsValue = JsString(obj.toString)

    override def read(json: JsValue): JobDetails.Source = json match {
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
    rootFormat(lazyFormat(jsonFormat(HttpJobArg.apply, "type", "args", "fields")))

  implicit val httpJobInfoF = rootFormat(lazyFormat(jsonFormat(HttpJobInfo.apply,
      "name", "execute", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

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

  implicit val httpJobInfoV2F = rootFormat(lazyFormat(jsonFormat(HttpFunctionInfoV2.apply,
    "name", "lang", "execute", "tags", "path", "className", "defaultContext")))


  implicit val workerInitInfoF = rootFormat(lazyFormat(jsonFormat(WorkerInitInfo.apply,
    "sparkConf", "maxJobs", "downtime", "streamingDuration", "logService", "masterAddress","masterHttpConf", "maxArtifactSize", "runOptions")))


  implicit val workerLinkF = rootFormat(lazyFormat(jsonFormat(WorkerLink.apply,
    "name", "address", "sparkUi", "initInfo")))

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
  implicit val gcMetricsF = jsonFormat2(GCMetrics.apply)
  implicit val threadMetricsF = jsonFormat6(ThreadMetrics.apply)
  implicit val heapF = jsonFormat4(Heap.apply)
  implicit val heapMetricsF = jsonFormat2(HeapMetrics.apply)
  implicit val javaVersionInfoF = jsonFormat2(JavaVersionInfo.apply)
  implicit val mistStatusF = jsonFormat7(MistStatus.apply)

  implicit val jobStartResponseF = jsonFormat1(JobStartResponse)

  implicit val runModeF = new JsonFormat[RunMode] {
    override def write(obj: RunMode): JsValue = {
      JsString(obj.name)
    }

    override def read(json: JsValue): RunMode = {
      json match {
        case JsString(name) => RunMode.fromName(name)
        case _ => throw new IllegalArgumentException(s"Can not extract RunMode from $json")
      }
    }
  }

  implicit val runSettingsF = jsonFormat1(RunSettings.apply)

  implicit val timeoutsF = jsonFormat2(Timeouts.apply)
  implicit val jobStartRequestF = jsonFormat6(FunctionStartRequest)
  implicit val asynJobStartRequestF = jsonFormat4(AsyncFunctionStartRequest)

  implicit val functionConfigF = jsonFormat4(FunctionConfig.apply)

  implicit val jobResultFormatF = jsonFormat3(JobResult.apply)

  implicit val logEventF = jsonFormat5(LogEvent.apply)

  implicit val devJobStartReqModelF = jsonFormat7(DevJobStartRequestModel.apply)


  implicit val contextConfigF = jsonFormat9(ContextConfig.apply)

  implicit val contextCreateRequestF = jsonFormat9(ContextCreateRequest.apply)
  implicit val jobDetailsResponseF = jsonFormat2(JobDetailsResponse.apply)

  implicit val updateEventF = new JsonFormat[SystemEvent] {

    implicit val iniF = jsonFormat5(InitializedEvent)
    implicit val queueF = jsonFormat1(QueuedEvent)
    implicit val startedF = jsonFormat2(StartedEvent)
    implicit val cancellingF = jsonFormat2(CancellingEvent)
    implicit val canceledF = jsonFormat2(CancelledEvent)
    implicit val finishedF = jsonFormat3(FinishedEvent)
    implicit val failedF = jsonFormat3(FailedEvent)

    implicit val receivedLogF = jsonFormat3(ReceivedLogs)
    implicit val fileDownloadingF = jsonFormat2(JobFileDownloadingEvent)
    implicit val workerAssigned = jsonFormat2(WorkerAssigned)

    object Keys {
      val initialized = "initialized"
      val queued = "queued"
      val started = "started"
      val jobFileDownloading = "job-file-downloading"
      val cancelling = "cancelling"
      val finished = "finished"
      val cancelled = "cancelled"
      val failed = "failed"
      val logs = "logs"
      val workerAssigned = "worker-assigned"
      val keepAlive = "keep-alive"
    }

    override def write(obj: SystemEvent): JsValue = {
      val (name, initial) = obj match {
        case x: InitializedEvent =>  Keys.initialized -> x.toJson
        case x: QueuedEvent => Keys.queued -> x.toJson
        case x: StartedEvent => Keys.started -> x.toJson
        case x: CancellingEvent => Keys.cancelling -> x.toJson
        case x: CancelledEvent => Keys.cancelled -> x.toJson
        case x: FinishedEvent => Keys.finished -> x.toJson
        case x: FailedEvent => Keys.failed -> x.toJson
        case x: ReceivedLogs => Keys.logs -> x.toJson
        case x: JobFileDownloadingEvent => Keys.jobFileDownloading -> x.toJson
        case x: WorkerAssigned => Keys.workerAssigned -> x.toJson
        case KeepAliveEvent => Keys.keepAlive -> JsObject(Map.empty[String, JsValue])
      }

      val merged = initial.asJsObject.fields + ("event" -> JsString(name))
      JsObject(merged)
    }

    override def read(json: JsValue): SystemEvent = {
      def fromName(obj: JsObject, v: String): SystemEvent = v match {
        case Keys.initialized => obj.convertTo[InitializedEvent]
        case Keys.queued => obj.convertTo[QueuedEvent]
        case Keys.started => obj.convertTo[StartedEvent]
        case Keys.cancelling => obj.convertTo[CancellingEvent]
        case Keys.cancelled => obj.convertTo[CancelledEvent]
        case Keys.finished => obj.convertTo[FinishedEvent]
        case Keys.failed => obj.convertTo[FailedEvent]
        case Keys.logs => obj.convertTo[ReceivedLogs]
        case Keys.workerAssigned => obj.convertTo[WorkerAssigned]
        case Keys.keepAlive => KeepAliveEvent
        case x => throw new IllegalArgumentException(s"Unknown event type $x")
      }

      val obj = json.asJsObject
      val name = obj.fields.getOrElse("event", JsNull)
      name match {
        case JsString(v) => fromName(obj, v)
        case _ => throw new IllegalArgumentException(s"Invalid event format $obj")
      }
    }
  }
}

object JsonCodecs extends JsonCodecs

