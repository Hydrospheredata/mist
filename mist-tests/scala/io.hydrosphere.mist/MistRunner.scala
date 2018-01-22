package io.hydrosphere.mist

import java.nio.file.Paths

import io.hydrosphere.mist.master.{JobResult, MasterAppArguments, MasterServer, ServerInstance}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scalaj.http.Http

trait MistRunner {

  private def getProperty(name: String): String = sys.props.get(name) match {
    case Some(v) => v
    case None => throw new RuntimeException(s"Property $name is not set")
  }

  val mistHome = getProperty("mistHome")
  val sparkHome = getProperty("sparkHome")
  val sparkVersion = getProperty("sparkVersion")

  def runMist(
    overrideConf: Option[String],
    overrideRouter: Option[String]
  ): ServerInstance = {
    def fromResource(path: String): String =
      getClass.getClassLoader.getResource(path).getPath

    val defaultArgs = MasterAppArguments.fromPath(Paths.get(mistHome))

    val conf = overrideConf.map(fromResource).getOrElse(defaultArgs.configPath)
    val router = overrideRouter.map(fromResource).getOrElse(defaultArgs.routerConfigPath)
    val starting = MasterServer.start(conf, router)
    Await.result(starting, Duration.Inf)
  }

}

object MistRunner extends MistRunner

trait MistItTest extends BeforeAndAfterAll with MistRunner { self: Suite =>

  val overrideConf: Option[String] = None
  val overrideRouter: Option[String] = None
  protected def beforeMistStart: Unit = {}

  var master: ServerInstance = _

  override def beforeAll {
    beforeMistStart
    master = runMist(overrideConf, overrideRouter)
  }

  override def afterAll: Unit = {
    Await.result(master.stop(), Duration.Inf)
  }

}

case class MistHttpInterface(
  host: String,
  port: Int,
  timeout: Int = 120
) {

  import io.hydrosphere.mist.master.interfaces.JsonCodecs._
  import spray.json.{enrichString, _}

  def runJob(routeId: String, params: (String, Any)*): JobResult =
    callV2Api(routeId, params.toMap)

  def serve(routeId: String, params: (String, Any)*): JobResult =
    callOldApi(routeId, params.toMap, Serve)

  private def callOldApi(
    routeId: String,
    params: Map[String, Any],
    action: ActionType): JobResult = {

    val millis = timeout * 1000

    val jobUrl = s"http://$host:$port/api/$routeId"
    val url = action match {
      case Serve => jobUrl + "?serve=true"
      case Execute => jobUrl
    }

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

  def callV2Api(
    endpointId: String,
    params: Map[String, Any]
  ): JobResult = {

    val millis = timeout * 1000
    val url = s"http://$host:$port/v2/api/endpoints/$endpointId/jobs?force=true"

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

