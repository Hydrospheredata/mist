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
      """mist.cluster.host = "0.0.0.0"
        |mist.cluster.public-host = "auto"
        |mist.http.host = "0.0.0.0"
        |mist.http.public-host = "auto"
        |mist.log-service.host = "auto"
        |mist.log-service.public-host = "5.5.5.5"
      """.stripMargin
    val cfg = ConfigFactory.parseString(cfgS)
    val masterConfig = MasterConfig.parseOnly("", MasterConfig.resolveUserConf(cfg))

    val auto = MasterConfig.autoConfigure(masterConfig, Now("1.1.1.1"))

    auto.cluster.host shouldBe "0.0.0.0"
    auto.cluster.publicHost shouldBe "1.1.1.1"
    auto.http.publicHost shouldBe "1.1.1.1"
    auto.http.host shouldBe "0.0.0.0"
    auto.logs.publicHost shouldBe "5.5.5.5"
  }
}
