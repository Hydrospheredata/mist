package io.hydrosphere.mist.master.artifact

import java.nio.file.Paths

import io.hydrosphere.mist.master.models.EndpointConfig

trait ArtifactKeyProvider[A, K] {
 def provideKey(a: A): K
}

class EndpointArtifactKeyProvider extends ArtifactKeyProvider[EndpointConfig, String]{
  override def provideKey(a: EndpointConfig): String = {
    import a._
    path match {
      case p if path.startsWith("hdfs") || path.startsWith("mvn") => p
      case p => Paths.get(p).getFileName.toString
    }
  }
}

