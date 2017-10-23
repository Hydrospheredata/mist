package io.hydrosphere.mist.master.interfaces.http

import java.time.LocalDateTime

import io.hydrosphere.mist.core.jvmjob.FullJobInfo
import io.hydrosphere.mist.master.models.ContextConfig
import mist.api.UserInputArgument

import scala.concurrent.duration.Duration

case class HttpJobInfo(
  name: String,
  execute: Option[Map[String, HttpJobArg]] = None,
  serve: Option[Map[String, HttpJobArg]] = None,

  isHiveJob: Boolean = false,
  isSqlJob: Boolean = false,
  isStreamingJob: Boolean = false,
  isMLJob: Boolean = false,
  isPython: Boolean = false
)


object HttpJobInfo {

  def forPython(name: String) = HttpJobInfo(name = name, isPython = true)

  def convert(info: FullJobInfo): HttpJobInfo = {
    val argsMap = info.execute
      .collect { case u: UserInputArgument => u }
      .map { a => a.name -> HttpJobArg.convert(a.t) }
      .toMap

    val jobInfo = HttpJobInfo(
      name = info.name,
      isPython = info.lang == FullJobInfo.PythonLang
    )

    if (info.isServe)
      jobInfo.copy(serve = Some(argsMap))
    else jobInfo.copy(execute = Some(argsMap))

  }
}

case class HttpJobArg(
  `type`: String,
  args: Seq[HttpJobArg]
)

object HttpJobArg {

  import mist.api.args._

  def convert(argType: ArgType): HttpJobArg = {
    val t = argType.getClass.getSimpleName.replace("$", "")
    val typeArgs = argType match {
      case x@(MInt | MDouble | MString | MAny) => Seq.empty
      case x: MMap => Seq(x.k, x.v).map(HttpJobArg.convert)
      case x: MList => Seq(HttpJobArg.convert(x.v))
      case x: MOption => Seq(HttpJobArg.convert(x.v))
    }
    new HttpJobArg(t, typeArgs)
  }
}


case class HttpEndpointInfoV2(
  name: String,
  lang: String,
  execute: Map[String, HttpJobArg] = Map.empty,

  tags: Seq[String] = Seq.empty,

  path: String,
  className: String,
  defaultContext: String

)

object HttpEndpointInfoV2 {

  //  val TagTraits = Seq(
  //    classOf[HiveSupport],
  //    classOf[SQLSupport],
  //    classOf[StreamingSupport],
  //    classOf[MLMistJob]
  //  )
  //
  //  case class TagTrait(clazz: Class[_], name: String)
  //
  //  val AllTags = Seq(
  //    TagTrait(classOf[HiveSupport], "hive"),
  //    TagTrait(classOf[SQLSupport], "sql"),
  //    TagTrait(classOf[StreamingSupport], "streaming"),
  //    TagTrait(classOf[MLMistJob], "ml")
  //  )

  def convert(info: FullJobInfo): HttpEndpointInfoV2 = {
    HttpEndpointInfoV2(
      name = info.name,
      path = info.path,
      className = info.className,
      defaultContext = info.defaultContext,
      execute = info.execute
        .collect { case a: UserInputArgument => a }
        .map(a => a.name -> HttpJobArg.convert(a.t))
        .toMap,
      lang = info.lang
    )
  }
}

case class EndpointCreateRequest(
  name: String,
  path: String,
  className: String,
  nameSpace: String
)

case class ContextCreateRequest(
  name: String,
  sparkConf: Option[Map[String, String]],
  downtime: Option[Duration],
  maxJobs: Option[Int],
  precreated: Option[Boolean],
  workerMode: Option[String],
  runOptions: Option[String] = None,
  streamingDuration: Option[Duration]
) {

  workerMode match {
    case Some(m) =>
      require(ContextCreateRequest.AvailableRunMode.contains(m),
        s"Worker mode should be in ${ContextCreateRequest.AvailableRunMode}")
    case _ =>
  }

  def toContextWithFallback(other: ContextConfig): ContextConfig =
    ContextConfig(
      name,
      sparkConf.getOrElse(other.sparkConf),
      downtime.getOrElse(other.downtime),
      maxJobs.getOrElse(other.maxJobs),
      precreated.getOrElse(other.precreated),
      runOptions.getOrElse(other.runOptions),
      workerMode.getOrElse(other.workerMode),
      streamingDuration.getOrElse(other.streamingDuration)
    )
}

object ContextCreateRequest {
  val AvailableRunMode = Set("shared", "exclusive")
}


case class MistStatus(
  mistVersion: String,
  sparkVersion: String,
  started: LocalDateTime
)

object MistStatus {

  import io.hydrosphere.mist.BuildInfo

  val Value = {
    val is1x = BuildInfo.sparkVersion.startsWith("1.")
    val sparkVersion = if (is1x) "1.x.x" else "2.x.x"
    MistStatus(
      BuildInfo.version,
      sparkVersion,
      LocalDateTime.now()
    )
  }
}

