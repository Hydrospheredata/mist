package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.{Source, Status}
import io.hydrosphere.mist.utils.TypeAlias.{JobResponse, JobResponseOrError}
import spray.json.{DeserializationException, JsObject, JsString, JsValue, RootJsonFormat}

import scala.language.reflectiveCalls

private[mist] trait JobDetailsJsonSerialization extends JobConfigurationJsonSerialization {
  
  implicit object EitherJobResultSupport extends RootJsonFormat[JobResponseOrError] {
    override def write(obj: JobResponseOrError): JsValue = obj match {
      case Right(s: String) => JsString(s)
      case Left(m: JobResponse) => mapFormat[String, Any].write(m)
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

  implicit val jobDetailsJsonFormat: RootJsonFormat[JobDetails] = jsonFormat7(JobDetails.apply)
  
}
