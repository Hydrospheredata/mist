package io.hydrosphere.mist.worker.runners
import java.io.File

import _root_.scala.concurrent.Future


trait ArtifactDownloader {
  def downloadArtifact(filePath: String): Future[File]
}

case class HttpArtifactDownloader(
  masterHttpHost: String,
  masterHttpPort: Int,
  savePath: String
) extends ArtifactDownloader {

  override def downloadArtifact(filePath: String): Future[File] = {
    def isRemote: Boolean = filePath.startsWith("mvn") || filePath.startsWith("hdfs")
    ???
  }
}

object ArtifactDownloader {

  def create(
    masterHttpHost: String,
    masterHttpPort: Int,
    savePath: String
  ): ArtifactDownloader = HttpArtifactDownloader(masterHttpHost, masterHttpPort, savePath)

}

