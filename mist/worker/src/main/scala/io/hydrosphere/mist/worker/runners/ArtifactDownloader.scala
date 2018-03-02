package io.hydrosphere.mist.worker.runners

import java.io.File
import java.net.URLEncoder
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, HttpMethods}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import io.hydrosphere.mist.utils.Logger
import org.apache.commons.codec.digest.DigestUtils

import _root_.scala.concurrent.Future

trait ArtifactDownloader {
  def downloadArtifact(filePath: String): Future[File]

  def stop(): Unit
}

case class HttpArtifactDownloader(
  masterHttpHost: String,
  masterHttpPort: Int,
  rootDir: Path,
  maxArtifactSize: Long
) extends ArtifactDownloader with Logger {

  implicit val system = ActorSystem("job-downloading")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  override def downloadArtifact(artifactKey: String): Future[File] = {
    val locallyResolvedFile = rootDir.resolve(artifactKey).toFile
    if (!locallyResolvedFile.exists()) {
      downloadFile(artifactKey)
    } else {
      for {
        valid <- checkHashes(artifactKey, locallyResolvedFile.toPath)
        _ = if (!valid) {
          logger.warn(s"Checksum of remote $artifactKey different from $locallyResolvedFile")
        }
      } yield locallyResolvedFile
    }
  }

  private def checkHashes(artifactKey: String, artifact: Path): Future[Boolean] = {
    for {
      checksum <- getChecksum(artifactKey)
      localFileChecksum = DigestUtils.sha1Hex(Files.newInputStream(artifact))
    } yield checksum == localFileChecksum
  }

  private def getChecksum(filePath: String): Future[String] = {
    val uri = s"http://$masterHttpHost:$masterHttpPort/v2/api/artifacts/${encode(filePath)}/sha"
    val request = HttpRequest(method = HttpMethods.GET, uri = uri)
    for {
      resp <- doRequest(request)
      checksum <- Unmarshal(resp.entity).to[String]
    } yield checksum
  }

  private def downloadFile(artifactKey: String): Future[File] = {
    val uri = s"http://$masterHttpHost:$masterHttpPort/v2/api/artifacts/${encode(artifactKey)}"
    val request = HttpRequest(method = HttpMethods.GET, uri = uri)
    for {
      resp <- doRequest(request)
      path = rootDir.resolve(artifactKey)
      _ <- resp.entity
        .withSizeLimit(maxArtifactSize)
        .dataBytes
        .runWith(FileIO.toPath(path))
      valid <- checkHashes(artifactKey, path)
      result = if (valid) {
        path
      } else {
        throw new IllegalArgumentException(s"Checksum of downloaded artifact $artifactKey different from $path")
      }
    } yield result.toFile
  }

  private def encode(filePath: String): String = URLEncoder.encode(filePath, "UTF-8")

  private def doRequest(request: HttpRequest): Future[HttpResponse] = for {
    r <- Http().singleRequest(request)
    resp = checkResponse(request.uri.toString(), r)
  } yield resp

  private def checkResponse(uri: String, resp: HttpResponse): HttpResponse = {
    if (resp.status.isSuccess()) resp
    else throw new IllegalArgumentException(s"Http error occurred in request $uri. Status ${resp.status}")
  }

  override def stop(): Unit = {
    system.terminate()
  }
}

object ArtifactDownloader {

  def create(
    masterHttpHost: String,
    masterHttpPort: Int,
    maxArtifactSize: Long,
    rootDir: Path
  ): ArtifactDownloader = HttpArtifactDownloader(
    masterHttpHost, masterHttpPort, rootDir, maxArtifactSize
  )

}

