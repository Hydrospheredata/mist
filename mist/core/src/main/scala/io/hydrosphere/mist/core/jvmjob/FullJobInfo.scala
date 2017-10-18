package io.hydrosphere.mist.core.jvmjob

import mist.api.UserInputArgument

case class FullJobInfo(
  lang: String,

  execute: Seq[UserInputArgument] = Seq.empty,
  tags: Seq[String] = Seq.empty,

  path: String,
  className: String,
  defaultContext: String = "default"
)
