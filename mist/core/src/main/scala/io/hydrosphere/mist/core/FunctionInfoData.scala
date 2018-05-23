package io.hydrosphere.mist.core

import mist.api.UserInputArgument

case class ExtractedFunctionData(
  name: String = "",
  lang: String = "",
  execute: Seq[UserInputArgument] = Seq.empty,
  isServe: Boolean = false,
  tags: Seq[String] = Seq.empty
)

case class FunctionInfoData(
  name: String,
  path: String,
  className: String,
  defaultContext: String,
  lang: String = "",
  execute: Seq[UserInputArgument] = Seq.empty,
  isServe: Boolean = false,
  tags: Seq[String] = Seq.empty
)

case object FunctionInfoData {
  val PythonLang = "python"
  val JavaLang = "java"
  val ScalaLang = "scala"
}
