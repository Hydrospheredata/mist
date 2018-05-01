package io.hydrosphere.mist.master

import cats.{Eval, Now}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
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
      """.stripMargin)

    val settings = ContextsSettings(cfg)
    settings.contexts.get("foo") shouldBe Some(ContextConfig(
      name = "foo",
      sparkConf = Map("spark.master" -> "local[2]"),
      downtime = Duration.Inf,
      maxJobs = 20,
      precreated = false,
      runOptions = "--opt",
      workerMode = RunMode.Shared,
      streamingDuration = 1.seconds,
      maxConnFailures = 5
    ))

  }

  it("should auto configure host") {
    val cfgS =
      """mist.cluster.host = "auto"
        |mist.http.host = "auto"
        |mist.log-service.host = "auto"
      """.stripMargin
    val cfg = ConfigFactory.parseString(cfgS)
    val masterConfig = MasterConfig.parse("", MasterConfig.resolveUserConf(cfg))

    val auto = MasterConfig.autoConfigure(masterConfig, Now("1.1.1.1"))

    auto.cluster.host shouldBe "1.1.1.1"
    auto.http.host shouldBe "1.1.1.1"
    auto.logs.host shouldBe "1.1.1.1"
  }
}
