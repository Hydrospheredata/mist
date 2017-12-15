package io.hydrosphere.mist.core.jvmjob

import mist.api.args.UserInputArgument

case class JobInfoData(
  name: String = "",
  lang: String = "",
  execute: Seq[UserInputArgument] = Seq.empty,
  isServe: Boolean = false,

  tags: Seq[String] = Seq.empty,

  path: String = "",
  className: String = "",
  defaultContext: String = "default"
)

case object JobInfoData {
  val PythonLang = "python"
  val JavaLang = "java"
  val ScalaLang = "scala"
}
