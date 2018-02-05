package io.hydrosphere.mist

import java.nio.file.{Files, Path}

import io.hydrosphere.mist.master.JobResult
import io.hydrosphere.mist.master.models.FunctionConfig

import scalaj.http._

case class MistHttpInterface(
  host: String,
  port: Int,
  timeout: Int = 120
) {

  import io.hydrosphere.mist.master.interfaces.JsonCodecs._
  import spray.json.{enrichString, _}

  def runJob(routeId: String, params: (String, Any)*): JobResult =
    callV2Api(routeId, params.toMap)

  def uploadArtifact(name: String, file: Path): Unit = {
    val bytes = Files.readAllBytes(file)
    val req = Http(s"http://$host:$port/v2/api/artifacts")
      .postMulti(MultiPart("file", name, "application/octet-stream", bytes))

    val resp = req.asBytes
    if (resp.code != 200)
      throw new RuntimeException(s"File $file uploading failed. Code: ${resp.code}, body: ${resp.body}")
  }

  def status: String = {
    val req = Http(s"http://$host:$port/v2/api/status")
    new String(req.asBytes.body)
  }

  def createFunction(ep: FunctionConfig): FunctionConfig = {
    val req = Http(s"http://$host:$port/v2/api/functions")
      .postData(ep.toJson)
    val resp = req.asString
    if (resp.code == 200)
      resp.body.parseJson.convertTo[FunctionConfig]
    else
      throw new RuntimeException(s"Function creation failed. Code: ${resp.code}, body: ${resp.body}")
  }

  def callV2Api(
    functionId: String,
    params: Map[String, Any]
  ): JobResult = {

    val millis = timeout * 1000
    val url = s"http://$host:$port/v2/api/functions/$functionId/jobs?force=true"

    val req = Http(url)
      .timeout(millis, millis)
      .header("Content-Type", "application/json")
      .postData(params.toJson)

    val resp = req.asString
    if (resp.code == 200)
      resp.body.parseJson.convertTo[JobResult]
    else
      throw new RuntimeException(s"Job failed body ${resp.body}")
  }

  sealed trait ActionType
  case object Execute extends ActionType
  case object Serve extends ActionType
}

