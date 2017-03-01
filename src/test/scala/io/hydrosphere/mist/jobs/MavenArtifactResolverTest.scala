package io.hydrosphere.mist.jobs

import java.nio.file.{Files, Paths}

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Flow
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

class MavenArtifactResolverTest extends FunSuite with Matchers {

  test("maven artifact") {
    val remote = MavenArtifact("io.hydrosphere", "mist", "0.0.1")
    remote.jarName shouldBe "mist-0.0.1.jar"
    remote.jarPath shouldBe "io/hydrosphere/mist/0.0.1/mist-0.0.1.jar"
  }

  test("construct resolver from path") {
    val path = "mvn://http://localhost:8081/artifactory/releases :: io.hydrosphere % mist_2.10 % 0.0.1"
    val resolver = MavenArtifactResolver.fromPath(path)
    resolver.repoUrl shouldBe "http://localhost:8081/artifactory/releases"
    resolver.artifact shouldBe MavenArtifact("io.hydrosphere", "mist_2.10", "0.0.1")
  }

  test("resolver over http") {
    import akka.http.scaladsl.model.StatusCodes._

    // maven-like repository mock
    val routes = Flow[HttpRequest].map { request =>
      val uri = request.uri.toString()
      if (uri.endsWith(".jar")) {
        HttpResponse(status = OK, entity = "JAR CONTENT")
      } else if (uri.endsWith(".sha1")) {
        val data = DigestUtils.sha1Hex("JAR CONTENT")
        HttpResponse(status = OK, entity = data)
      } else {
        HttpResponse(status = NotFound, entity = s"Not found ${request.uri}")
      }
    }

    val future = MockHttpServer.onServer(routes, binding => {
      val port = binding.localAddress.getPort
      val url = s"http://localhost:$port/artifactory/libs-release-local"
      val artifact = MavenArtifact("mist_examples", "mist_examples_2.10", "0.8.0")
      val resolver = MavenArtifactResolver(url, artifact, "target")
      resolver.file
    })

    val file = Await.result(future, Duration.Inf)
    file.exists() shouldBe true
    val content = Files.readAllBytes(Paths.get(file.getAbsolutePath))
    new String(content) shouldBe "JAR CONTENT"
  }

  object MockHttpServer {

    import akka.actor.ActorSystem
    import akka.http.scaladsl.Http
    import akka.stream.ActorMaterializer
    import akka.util.Timeout

    import scala.concurrent.duration._

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
        try{
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
}
