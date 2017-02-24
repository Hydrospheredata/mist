package io.hydrosphere.mist.utils.json

import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.{Source, Status}
import spray.json.{JsObject, JsString, JsValue, RootJsonFormat}

import scala.language.reflectiveCalls

private[mist] trait JobDetailsJsonSerialization extends JobConfigurationJsonSerialization {
  
  implicit object EitherJobResultSupport extends RootJsonFormat[Either[String, Map[String, Any]]] {
    override def write(obj: Either[String, Map[String, Any]]): JsValue = obj match {
      case Left(s: String) => JsString(s)
      case Right(m: Map[String, Any]) => mapFormat[String, Any].write(m)
    }

    override def read(json: JsValue): Either[String, Map[String, Any]] = json match {
      case JsString(str) => Left(str)
      case _: JsObject => Right(mapFormat[String, Any].read(json))
    }
  }
  
  
  implicit object JobStatusSupport extends RootJsonFormat[JobDetails.Status] {
    override def write(obj: Status): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Status = json match {
      case JsString(str) => JobDetails.Status(str)
    }
  }
  
  implicit object JobSourceSupport extends RootJsonFormat[JobDetails.Source] {
    override def write(obj: Source): JsValue = JsString(obj.toString)

    override def read(json: JsValue): Source = json match {
      case JsString(str) => JobDetails.Source(str)
    }
  }

  implicit val jobDetailsJsonFormat: RootJsonFormat[JobDetails] = jsonFormat7(JobDetails.apply)
  
}
