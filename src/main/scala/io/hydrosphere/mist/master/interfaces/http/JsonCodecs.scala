package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import io.hydrosphere.mist.master.models.JobStartResponse
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.json.{AnyJsonFormatSupport, JobDetailsJsonSerialization}
import spray.json._

trait JsonCodecs extends SprayJsonSupport
  with DefaultJsonProtocol
  with AnyJsonFormatSupport
  with JobDetailsJsonSerialization {

  implicit val printer = CompactPrinter

  //implicit val jobExecutionStatusF = jsonFormat5(JobExecutionStatus)
  implicit val httpJobArgF: RootJsonFormat[HttpJobArg] =
    rootFormat(lazyFormat(jsonFormat(HttpJobArg.apply, "type", "args")))

  implicit val httpJobInfoF = rootFormat(lazyFormat(jsonFormat(HttpJobInfo.apply,
      "name", "execute", "train", "serve",
      "isHiveJob", "isSqlJob","isStreamingJob", "isMLJob", "isPython")))

  implicit val httpJobInfoV2F = rootFormat(lazyFormat(jsonFormat(HttpJobInfoV2.apply,
    "name", "lang", "execute", "train", "serve", "tags")))

  implicit val workerLinkF = jsonFormat2(WorkerLink)

  implicit val jobStartResponseF = jsonFormat1(JobStartResponse)

}

object JsonCodecs extends JsonCodecs

