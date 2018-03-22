package io.hydrosphere.mist.worker.runners

import java.nio.file.{Files, Paths}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.Flow
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}

class ArtifactDownloaderSpec extends FunSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  val basePath = Paths.get("./target/artifacts")

  it("should create SimpleArtifactDownloader") {
    val downloader = ArtifactDownloader.create("localhost", 2004, 262144000, basePath)
    downloader shouldBe a[HttpArtifactDownloader]
  }

  describe("SimpleArtifactDownloader") {
    val routes = Flow[HttpRequest].map { request =>
      val uri = request.uri.toString()
      if (uri.endsWith(".jar")) {
        HttpResponse(status = StatusCodes.OK, entity = "JAR CONTENT")
      } else if (uri.endsWith("/sha")) {
        val data = DigestUtils.sha1Hex("JAR CONTENT")
        HttpResponse(status = StatusCodes.OK, entity = data)
      } else {
        HttpResponse(status = StatusCodes.NotFound, entity = s"Not found ${request.uri}")
      }
    }

    before {
      val f = basePath.toFile
      FileUtils.deleteQuietly(f)
      FileUtils.forceMkdir(f)
    }

    it("should download file if it not found locally") {
      val fileContent = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, 262144000, basePath)
        val artifact = Await.result(downloader.downloadArtifact("test.jar"), Duration.Inf)
        new String(Files.readAllBytes(artifact.local.toPath))
      })

      Await.result(fileContent, Duration.Inf) shouldBe "JAR CONTENT"
    }
    it("should not download file if sha of local file and remote not equal") {
      val localFile = basePath.resolve("test.jar")
      Files.write(localFile, "DIFFERENT".getBytes())

      val fileContent = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, 262144000, basePath)
        val file = Await.result(downloader.downloadArtifact("test.jar"), Duration.Inf)
        file
      })
      Await.result(fileContent, Duration.Inf).local.lastModified() shouldBe localFile.toFile.lastModified()
    }

    it("should not download file if checksums are correct") {
      val localFile = basePath.resolve("test.jar")
      Files.write(localFile, "JAR CONTENT".getBytes())

      val fileF = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, 262144000, basePath)
        val file = Await.result(downloader.downloadArtifact("test.jar"), Duration.Inf)
        file
      })
      Await.result(fileF, Duration.Inf).local.lastModified() == localFile.toFile.lastModified()
    }

    it("should fail when local and remote file not found") {
      val routes = Flow[HttpRequest].map {request => {
        HttpResponse(status = StatusCodes.NotFound, entity = s"Not found ${request.uri}")
      }}

      val fileF = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, 262144000, basePath)

        val fileF = downloader.downloadArtifact("test.jar")
        Await.result(fileF, Duration.Inf)
      })

      intercept[IllegalArgumentException] {
        Await.result(fileF, 30.seconds)
      }
    }
  }

}

object MockHttpServer {

  import akka.actor.ActorSystem
  import akka.stream.ActorMaterializer
  import akka.util.Timeout

  import scala.concurrent.duration._
  import scala.concurrent.{Future, Promise}

  def onServer[A](
    routes: Flow[HttpRequest, HttpResponse, _],
    f: (Http.ServerBinding) => A): Future[A] = {

    implicit val system = ActorSystem("mock-http-cli")
    implicit val materializer = ActorMaterializer()

    implicit val executionContext = system.dispatcher
    implicit val timeout = Timeout(1.seconds)

    val binding = Http().bindAndHandle(routes, "localhost", 0)

    val close = Promise[Http.ServerBinding]
    close.future
      .flatMap(binding => binding.unbind())
      .onComplete(_ => {
        materializer.shutdown()
        Await.result(system.terminate(), Duration.Inf)
      })

    val result = binding.flatMap(binding => {
      try {
        Future.successful(f(binding))
      } catch {
        case e: Throwable =>
          Future.failed(e)
      } finally {
        close.success(binding)
      }
    })
    result
  }
}
