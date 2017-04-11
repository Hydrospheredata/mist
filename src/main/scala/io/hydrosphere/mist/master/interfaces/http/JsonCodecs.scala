package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshal, Marshaller}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status
import io.hydrosphere.mist.master.{JobExecutionStatus, WorkerLink}
import io.hydrosphere.mist.utils.json.{JobDetailsJsonSerialization, AnyJsonFormatSupport}
import spray.json._

trait JsonCodecs extends SprayJsonSupport
  with DefaultJsonProtocol
  with AnyJsonFormatSupport
  with JobDetailsJsonSerialization {

  implicit val printer = CompactPrinter

  implicit val jobExecutionStatusF = jsonFormat4(JobExecutionStatus)
  implicit val httpJobArgF: RootJsonFormat[HttpJobArg] =
    rootFormat(lazyFormat(jsonFormat(HttpJobArg.apply, "type", "args")))

  implicit val httpJobInfoF = rootFormat(lazyFormat(jsonFormat(HttpJobInfo.apply,
      "execute", "train", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

  implicit val workerLinkF = jsonFormat2(WorkerLink)

}

object JsonCodecs extends JsonCodecs

