package io.hydrosphere.mist.master.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshal, Marshaller}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.json.AnyJsonFormatSupport
import spray.json._

trait JsonCodecs extends SprayJsonSupport
  with DefaultJsonProtocol
  with AnyJsonFormatSupport {

  implicit object JobStatusSupport extends RootJsonFormat[JobDetails.Status] {
    override def write(obj: Status): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Status = json match {
      case JsString(str) => JobDetails.Status(str)
      case _ => throw DeserializationException("JobDetails.Status must be a string")
    }
  }

  implicit val jobExecutionStatusF = jsonFormat3(JobExecutionStatus)
  implicit val httpJobArgF: RootJsonFormat[HttpJobArg] =
    jsonFormat(HttpJobArg.apply, "type", "args")

  implicit val httpJobInfoF = rootFormat(lazyFormat(
    jsonFormat(HttpJobInfo.apply,
      "execute", "train", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

  implicit val workerLinkF = jsonFormat4(WorkerLink)

}

object JsonCodecs extends JsonCodecs

