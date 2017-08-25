package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.master.models.RunMode
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class WorkerRunnersSpec extends FunSpec with Matchers{

  describe("Shell args") {

    it("should build arguments for worker") {
      val cfg = MasterConfig(
        cluster = HostPortConfig("0.0.0.0", 2345),
        http = HttpConfig("0.0.0.0", 2004, "path"),
        mqtt = AsyncInterfaceConfig.disabled,
        kafka = AsyncInterfaceConfig.disabled,
        logs = LogServiceConfig("logsHost", 5000, ""),
        workers = WorkersSettingsConfig("local", 20 seconds, "", 0, "", ""),
        contextsSettings = ContextsSettings(contextConfig),
        dbPath = "",
        security = SecurityConfig.disabled,
        raw = ConfigFactory.empty(),
        contextsPath = "",
        endpointsPath = ""
      )

      val name = "worker-name"
      val context = cfg.contextsSettings.contexts.get("foo").get
      val mode = RunMode.Shared


      val result = ShellWorkerScript.workerArgs(name, context, mode, cfg)
        val x = Seq(
        "--master", "0.0.0.0:2345",
        "--name", "worker-name",
        "--context-name", "foo",
        "--mode", "shared",
        "--run-options", "--opt"
      )
      result should contain theSameElementsAs x
    }
  }

  val contextConfig = ConfigFactory.parseString(
    """
      |context-defaults {
      | downtime = Inf
      | streaming-duration = 1 seconds
      | max-parallel-jobs = 20
      | precreated = false
      | worker-mode = "shared"
      | spark-conf = { }
      | run-options = "--opt"
      |}
      |
      |context {
      |
      |  foo {
      |    spark-conf {
      |       spark.master = "local[2]"
      |       key = value
      |    }
      |  }
      |}
    """.stripMargin)
}
