package io.hydrosphere.mist.master.artifact

import io.hydrosphere.mist.master.models.EndpointConfig
import org.apache.commons.io.FilenameUtils

trait ArtifactKeyProvider[A, K] {
 def provideKey(a: A): K
}

class EndpointArtifactKeyProvider extends ArtifactKeyProvider[EndpointConfig, String]{
  override def provideKey(a: EndpointConfig): String = {
    import a._

    path match {
      case p if path.startsWith("hdfs") || path.startsWith("mvn") => p
      case p => FilenameUtils.getName(p)
    }
  }
}

