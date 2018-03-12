package io.hydrosphere.mist.worker

import java.io.File

case class SparkArtifact(
  local: File,
  url: String
)

