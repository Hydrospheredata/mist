package io.hydrosphere.mist.master.interfaces.http

import java.time.LocalDateTime

import io.hydrosphere.mist.api._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.jar._
import io.hydrosphere.mist.master.models.{ContextConfig, FullEndpointInfo, RunMode}

import scala.concurrent.duration.Duration

case class HttpJobInfo(
  name: String,
  execute: Option[Map[String, HttpJobArg]] = None,
  serve:   Option[Map[String, HttpJobArg]] = None,

  isHiveJob: Boolean = false,
  isSqlJob: Boolean = false,
  isStreamingJob: Boolean = false,
  isMLJob: Boolean = false,
  isPython: Boolean = false
)


object HttpJobInfo {

  def forPython(name: String) = HttpJobInfo(name = name, isPython = true)

  def convert(fullInfo: FullEndpointInfo): HttpJobInfo = fullInfo.info match {
    case PyJobInfo => HttpJobInfo.forPython(fullInfo.config.name)
    case jvm: JvmJobInfo =>
      val inst = jvm.jobClass
      val classes = inst.supportedClasses()
      HttpJobInfo(
        name = fullInfo.config.name,
        execute = inst.execute.map(i => i.argumentsTypes.mapValues(HttpJobArg.convert)),
        serve = inst.serve.map(i => i.argumentsTypes.mapValues(HttpJobArg.convert)),

        isHiveJob = classes.contains(classOf[HiveSupport]),
        isSqlJob = classes.contains(classOf[SQLSupport]),
        isStreamingJob = classes.contains(classOf[StreamingSupport]),
        isMLJob = classes.contains(classOf[MLMistJob])
      )
  }
}

case class HttpJobArg(
  `type`: String,
  args: Seq[HttpJobArg]
)

object HttpJobArg {

  def convert(argType: JobArgType): HttpJobArg = {
    val t = argType.getClass.getSimpleName.replace("$", "")
    val typeArgs = argType match {
      case x @ (MInt | MDouble| MString | MAny) => Seq.empty
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
  execute: Option[Map[String, HttpJobArg]] = None,

  tags: Seq[String] = Seq.empty,

  path: String,
  className: String,
  defaultContext: String

)

object HttpEndpointInfoV2 {

  val PyLang = "python"
  val ScalaLang = "scala"

  val TagTraits = Seq(
    classOf[HiveSupport],
    classOf[SQLSupport],
    classOf[StreamingSupport],
    classOf[MLMistJob]
  )

  case class TagTrait(clazz: Class[_], name: String)

  val AllTags = Seq(
    TagTrait(classOf[HiveSupport], "hive"),
    TagTrait(classOf[SQLSupport], "sql"),
    TagTrait(classOf[StreamingSupport], "streaming"),
    TagTrait(classOf[MLMistJob], "ml")
  )

  def convert(fullInfo: FullEndpointInfo): HttpEndpointInfoV2 = {
    import fullInfo.config._

    fullInfo.info match {
      case PyJobInfo => HttpEndpointInfoV2(
        name = name, lang = PyLang,
        path = path,
        className = className,
        defaultContext = defaultContext
      )
      case jvm: JvmJobInfo =>
        val inst = jvm.jobClass
        val classes = inst.supportedClasses()
        val tags = AllTags.filter(tag => classes.contains(tag.clazz)).map(_.name)
        HttpEndpointInfoV2(
          name = name,
          lang = ScalaLang,
          execute = inst.execute.map(i => i.argumentsTypes.mapValues(HttpJobArg.convert)),
          tags = tags,

          path = path,
          className = className,
          defaultContext = defaultContext
        )
    }
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

