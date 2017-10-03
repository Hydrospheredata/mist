package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.master.models.ContextConfig
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class MistConfigSpec extends FunSpec with Matchers {
  it("parse context settings") {

    val cfg = ConfigFactory.parseString(
      """
        |context-defaults {
        | downtime = Inf
        | streaming-duration = 1 seconds
        | max-parallel-jobs = 20
        | precreated = false
        | spark-conf = { }
        | run-options = "--opt"
        | worker-mode = "shared"
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
      """.stripMargin)

    val settings = ContextsSettings(cfg)
    settings.contexts.get("foo") shouldBe Some(ContextConfig(
      name = "foo",
      sparkConf = Map("spark.master" -> "local[2]"),
      downtime = Duration.Inf,
      maxJobs = 20,
      precreated = false,
      runOptions = "--opt",
      workerMode = "shared",
      streamingDuration = 1.seconds
    ))

  }
}
