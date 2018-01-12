package io.hydrosphere.mist.worker.runners

import java.io.File
import java.net.URLEncoder
import java.nio.file.{Files, Paths}
import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.FileIO
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FilenameUtils

import _root_.scala.concurrent.{ExecutionContext, Future}

trait ArtifactDownloader {
  def downloadArtifact(filePath: String): Future[File]
  def stop(): Unit
}

case class HttpArtifactDownloader(
  masterHttpHost: String,
  masterHttpPort: Int,
  savePath: String
) extends ArtifactDownloader {

  implicit val system = ActorSystem("job-downloading")
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  override def downloadArtifact(filePath: String): Future[File] = {
    val absFile = new File(filePath)
    val locallyResolvedFile = Paths.get(savePath, filePath).toFile

    filePath match {
      case _ if absFile.exists() =>
        Future.successful(absFile)
      case filePath if locallyResolvedFile.exists() =>
        for {
          checksum          <- getChecksum(filePath)
          localFileChecksum =  DigestUtils.sha1Hex(Files.newInputStream(locallyResolvedFile.toPath))
          file              <- if (checksum == localFileChecksum)
                                 Future.successful(locallyResolvedFile)
                               else downloadFile(filePath)
        } yield file
      case filePath =>
        downloadFile(filePath)
    }
  }

  private def getChecksum(filePath: String): Future[String] = {
    val uri = s"http://$masterHttpHost:$masterHttpPort/v2/api/artifacts/${encode(filePath)}/sha"
    val request = HttpRequest(method = HttpMethods.GET, uri = uri)
    for {
      r        <- Http().singleRequest(request)
      resp     =  checkResponse(uri, r)
      checksum <- Unmarshal(resp.entity).to[String]
    } yield checksum
  }

  private def downloadFile(filePath: String): Future[File] = {
    val uri = s"http://$masterHttpHost:$masterHttpPort/v2/api/artifacts/${encode(filePath)}"
    val request = HttpRequest(method = HttpMethods.GET, uri = uri)
    for {
      r    <- Http().singleRequest(request)
      resp =  checkResponse(uri, r)
      file =  localFile(filePath)
      _    <- resp.entity.dataBytes.runWith(FileIO.toFile(file))
    } yield file
  }

  private def encode(filePath: String): String = URLEncoder.encode(filePath, "UTF-8")

  private def checkResponse(uri: String, resp: HttpResponse): HttpResponse = {
    if (resp.status.isSuccess()) resp
    else throw new IllegalArgumentException(s"Http error occurred in request $uri. Status ${resp.status}")
  }

  private def localFile(filePath: String): File = {
    def isRemote: Boolean = filePath.startsWith("mvn://") || filePath.startsWith("hdfs://")

    val fileName = if (isRemote)
      UUID.randomUUID().toString + ".jar"
    else
      FilenameUtils.getName(filePath)

    Paths.get(savePath, fileName).toFile
  }

  override def stop(): Unit = {
    system.terminate()
  }
}

object ArtifactDownloader {

  def create(
    masterHttpHost: String,
    masterHttpPort: Int,
    savePath: String
  ): ArtifactDownloader = HttpArtifactDownloader(masterHttpHost, masterHttpPort, savePath)

}

