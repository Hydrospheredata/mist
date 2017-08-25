package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.JobResult
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import org.scalatest._
import scala.sys.process._

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
  ): Process = {

    def fromResource(path: String): String =
      getClass.getClassLoader.getResource(path).getPath

    def optArg(key: String, value: Option[String]): Seq[String] =
      value.map(v => Seq(key, v)).getOrElse(Seq.empty)

    val configArg = optArg("--config", overrideConf.map(fromResource))
    val routerArg = optArg("--router-config", overrideRouter.map(fromResource))
    val args = Seq(s"$mistHome/bin/mist-master", "start") ++ configArg ++ routerArg

    val env = sys.env.toSeq :+ ("SPARK_HOME" -> sparkHome)
//    val ps = Process(args, None, env: _*).run(new ProcessLogger {
//      override def buffer[T](f: => T): T = f
//
//      override def out(s: => String): Unit = ()
//
//      override def err(s: => String): Unit = ()
//    })
    val ps = Process(args, None, env: _*).run()
    Thread.sleep(5000)
    ps
  }

  def killMist(): Unit ={
    Process(s"$mistHome/bin/mist-master stop").run(false).exitValue()
  }
}

object MistRunner extends MistRunner

trait MistItTest extends BeforeAndAfterAll with MistRunner { self: Suite =>

  val overrideConf: Option[String] = None
  val overrideRouter: Option[String] = None

  private var ps: Process = null

  override def beforeAll {
    ps = runMist(overrideConf, overrideRouter)
  }

  override def afterAll: Unit = {
    // call kill over bash - destroy works strangely
    ps.destroy()
    killMist()
    Thread.sleep(5000)
  }

  def isSpark2: Boolean = sparkVersion.startsWith("2.")
  def isSpark1: Boolean = !isSpark2

  def runOnlyIf(f: => Boolean, descr: String)(body: => Unit) = {
    if (f) body
    else cancel(descr)
  }

  def runOnlyOnSpark2(body: => Unit): Unit =
    runOnlyIf(isSpark2, "SKIP TEST - ONLY FOR SPARK2")(body)

  def runOnlyOnSpark1(body: => Unit): Unit =
    runOnlyIf(isSpark1, "SKIP TEST - ONLY FOR SPARK1")(body)
}

case class MistHttpInterface(
  host: String,
  port: Int,
  timeout: Int = 120
) {

  import JsonCodecs._
  import spray.json.pimpString
  import spray.json._
  import scalaj.http.Http

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

