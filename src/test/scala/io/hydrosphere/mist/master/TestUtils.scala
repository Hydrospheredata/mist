package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object TestUtils {

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


  implicit class AwaitSyntax[A](f: => Future[A]) {
    def await: A = Await.result(f, Duration.Inf)
  }

}
