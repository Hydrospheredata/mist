package io.hydrosphere.mist.master.http

import io.hydrosphere.mist.jobs.runners.jar._

case class HttpJobInfo(
  execute: Option[Map[String, HttpJobArg]] = None,
  train:   Option[Map[String, HttpJobArg]] = None,
  serve:   Option[Map[String, HttpJobArg]] = None,

  isHiveJob: Boolean = false,
  isSqlJob: Boolean = false,
  isStreamingJob: Boolean = false,
  isMLJob: Boolean = false,
  isPython: Boolean = false
)

object HttpJobInfo {

  def forPython() = HttpJobInfo(isPython = true)
}

case class HttpJobArg(
  `type`: String,
  args: Seq[HttpJobArg]
)

object HttpJobArg {

  def convert(argType: JobArgType): HttpJobArg = {
    val t = argType.getClass.getSimpleName.replace("$", "")
    val typeArgs = argType match {
      case x @ (MInt | MDouble| MString) => Seq.empty
      case x: MMap => Seq(x.k, x.v).map(HttpJobArg.convert)
      case x: MList => Seq(HttpJobArg.convert(x.v))
      case x: MOption => Seq(HttpJobArg.convert(x.v))
    }
    new HttpJobArg(t, typeArgs)
  }
}


