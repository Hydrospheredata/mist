package io.hydrosphere.mist.master

import io.hydrosphere.mist.core.CommonData.{Action, JobParams}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import JsonCodecs._
import mist.api.data.{JsData, JsMap}
import spray.json.{JsObject, JsString, enrichAny, enrichString}

/**
  * This class is mapped to the job_details table with 1:1 table column to
  * class field.
  *
  * Class intended as a helper for Doobie library and used in HikariJobRepository
  *
  * @see HikariJobRepository
  * @author Andrew G. Saushkin
  */
case class JobDetailsRecord (
  path: String, className: String, namespace: String, parameters: String,
  externalId: Option[String], function: Option[String], action: String,
  source: String, jobId: String, startTime: Option[Long], endTime: Option[Long],
  jobResult: Option[String], status: String, workerId: Option[String],
  createTime: Long) {

  def toJobDetails: JobDetails = {
    JobDetails(function.get, jobId,
      JobParams(path, className, parameters.parseJson.convertTo[JsMap], toAction(action)),
      namespace, externalId, JobDetails.Source(source), startTime, endTime,
      toResult(jobResult), JobDetails.Status(status), workerId, createTime)
  }

  def toResult(stringOption: Option[String]): Option[Either[String, JsData]] = {
    stringOption match {
      case Some(string) => string.parseJson match {
          case obj @ JsObject(fields) =>
            val maybeErr = fields.get("error").flatMap({
              case JsString(err) => Some(err)
              case x => None
            })
            maybeErr match {
              case None => Some(Right(fields.get("result").get.convertTo[JsData]))
              case Some(err) => Some(Left(err))
            }
          case JsString(err) => Some(Left(err))
          case _ => throw new IllegalArgumentException(s"can not deserialize $string to Job response")
        }
      case None => None
    }
  }

  def toAction(action: String) = {
    action match {
      case "serve" => Action.Serve
      case "train" => Action.Execute
      case _ => Action.Execute
    }
  }
}

object JobDetailsRecord {
  def apply(jd: JobDetails): JobDetailsRecord = {
    val jp: JobParams = jd.params
    new JobDetailsRecord(jp.filePath, jp.className, jd.context,
      jp.arguments.toJson.compactPrint, jd.externalId, Some(jd.function),
      jp.action.toString, jd.source.toString, jd.jobId, jd.startTime, jd.endTime,
      jobResponseToString(jd.jobResult), jd.status.toString, jd.workerId,
      jd.createTime
    )
  }

  def jobResponseToString(jobResponseOrError: Option[Either[String, JsData]]): Option[String] = {
    jobResponseOrError match {
      case Some(response) => val jsValue = response match {
          case Left(err) => JsObject("error" -> JsString(err))
          case Right(data) => JsObject("result" -> data.toJson)
        }
        Some(jsValue.compactPrint)
      case None => None
    }
  }
}
