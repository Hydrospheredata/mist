package io.hydrosphere.mist.worker.runners

import java.io.File
import java.nio.file.{Files, Paths}

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.stream.scaladsl.Flow
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FileUtils
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpecLike, Matchers}

import _root_.scala.concurrent.Await
import _root_.scala.concurrent.duration.Duration

class ArtifactDownloaderSpec extends FunSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  val basePath = "./target/artifacts"

  it("should create SimpleArtifactDownloader") {
    val downloader = ArtifactDownloader.create("localhost", 2004, basePath)
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
      val f = new File(basePath)
      FileUtils.deleteQuietly(f)
    }

    it("should download file if it not found locally") {
      val fileContent = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, basePath)
        val file = Await.result(downloader.downloadArtifact("test.jar"), Duration.Inf)
        new String(Files.readAllBytes(file.toPath))
      })

      Await.result(fileContent, Duration.Inf) shouldBe "JAR CONTENT"
    }
    it("should download file if sha of local file and remote not equal") {
      Files.write(Paths.get(basePath, "test.jar"), "DIFFERENT".getBytes())

      val fileContent = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, basePath)
        val file = Await.result(downloader.downloadArtifact("test.jar"), Duration.Inf)
        new String(Files.readAllBytes(file.toPath))
      })
      Await.result(fileContent, Duration.Inf) shouldBe "JAR CONTENT"
    }
    it("should fail when local and remote file not found") {
      val routes = Flow[HttpRequest].map {request => {
        HttpResponse(status = StatusCodes.NotFound, entity = s"Not found ${request.uri}")
      }}

      val fileF = MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, basePath)

        val fileF = downloader.downloadArtifact("test.jar")
        Await.result(fileF.failed, Duration.Inf)
      })

      ScalaFutures.whenReady(fileF.failed) {ex =>
        ex shouldBe a[IllegalArgumentException]
      }
    }

    it("should download handle all types of path: mvn, hdfs") {

      val routes = Flow[HttpRequest].map {request =>
        HttpResponse(status = StatusCodes.OK, entity = "JARJAR")
      }

      val mvnPath = "mvn://http://localhost:8081/artifactory/releases :: io.hydrosphere % mist_2.10 % 0.0.1"
      val hdfsPath = "hdfs://localhost:0/test.jar"

      val (f, f2) = Await.result(MockHttpServer.onServer(routes, binding => {
        val port = binding.localAddress.getPort
        val downloader = ArtifactDownloader.create("localhost", port, basePath)
        val f = Await.result(downloader.downloadArtifact(mvnPath), Duration.Inf)
        val f2 = Await.result(downloader.downloadArtifact(hdfsPath), Duration.Inf)
        (f, f2)
      }), Duration.Inf)

      f.getName should endWith (".jar")
      f2.getName should endWith (".jar")
    }
  }

}

object MockHttpServer {

  import akka.actor.ActorSystem
  import akka.stream.ActorMaterializer
  import akka.util.Timeout

  import _root_.scala.concurrent.duration._
  import _root_.scala.concurrent.{Future, Promise}

  def onServer[A](
    routes: Flow[HttpRequest, HttpResponse, Unit],
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
        system.shutdown()
        system.awaitTermination()
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
