package io.hydrosphere.mist.master

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.scaladsl.Flow
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future, Promise}

trait TestUtils {

  implicit class AwaitSyntax[A](f: => Future[A]) {
    def await: A = Await.result(f, Duration.Inf)
    def await(d: FiniteDuration): A = Await.result(f, d)
  }

}
object TestUtils extends TestUtils {

  val cfgStr =
    """
      |context-defaults {
      | downtime = Inf
      | streaming-duration = 1 seconds
      | max-parallel-jobs = 20
      | precreated = false
      | spark-conf = { }
      | worker-mode = "shared"
      | run-options = "--opt"
      | max-conn-failures = 5
      |}
      |
      |context {
      |
      |  foo {
      |    spark-conf {
      |       spark.master = "local[2]"
      |    }
      |  }
      |}
    """.stripMargin

  val contextSettings = {
    val cfg = ConfigFactory.parseString(cfgStr)
    ContextsSettings(cfg)
  }

  val FooContext = contextSettings.contexts.get("foo").get




  object MockHttpServer {

    import akka.actor.ActorSystem
    import akka.http.scaladsl.Http
    import akka.stream.ActorMaterializer
    import akka.util.Timeout

    import scala.concurrent.duration._

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

}
