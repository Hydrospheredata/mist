package io.hydrosphere.mist.master.data

import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigParseOptions, ConfigRenderOptions}
import io.hydrosphere.mist.master.models.{ContextConfig, FunctionConfig, RunMode}
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class ConfigReprsSpec extends FunSpec with Matchers {

  import ConfigRepr._

  describe("EndpointConfig") {

    it("should be parsed from config") {
      val cfg = ConfigFactory.parseString(
        """
         |  path = "jar_path.jar"
         |  className = "MyJob1"
         |  namespace = "namespace"
      """.stripMargin)

      val parsed = cfg.to[FunctionConfig]("name")
      parsed shouldBe FunctionConfig("name", "jar_path.jar", "MyJob1", "namespace")
    }

    it("should render to raw") {
      val e = FunctionConfig("name", "jar_path.jar", "MyJob1", "namespace")
      val raw = e.toConfig

      raw.getString("path") shouldBe "jar_path.jar"
      raw.getString("className") shouldBe "MyJob1"
      raw.getString("namespace") shouldBe "namespace"
    }
  }

  describe("ContextConfig") {

    it("should be parsed from config") {
      val cfg = ConfigFactory.parseString(
        """
          |{
          |  spark-conf {
          |    "x.y" = "z"
          |    a = 1
          |  }
          |  downtime = Inf
          |  max-parallel-jobs = 100
          |  precreated = false
          |  run-options = "--key"
          |  streaming-duration = 30 seconds
          |  worker-mode = "shared"
          |  max-conn-failures = 10
          |
          |}
        """.stripMargin
      , ConfigParseOptions.defaults())

      val context = cfg.to[ContextConfig]("test")
      context.sparkConf shouldBe Map("x.y" -> "z", "a" -> "1")
      context.downtime shouldBe Duration.Inf
      context.maxJobs shouldBe 100
      context.precreated shouldBe false
      context.runOptions shouldBe "--key"
      context.workerMode shouldBe RunMode.Shared
      context.streamingDuration shouldBe 30.seconds
      context.maxConnFailures shouldBe 10
    }

    it("should ignore missing max failures") {
      val cfg = ConfigFactory.parseString(
        """
          |{
          |  spark-conf {
          |    "x.y" = "z"
          |    a = 1
          |  }
          |  downtime = Inf
          |  max-parallel-jobs = 100
          |  precreated = false
          |  run-options = "--key"
          |  streaming-duration = 30 seconds
          |  worker-mode = "shared"
          |
          |}
        """.stripMargin
        , ConfigParseOptions.defaults())

      val context = cfg.to[ContextConfig]("test")
      context.maxConnFailures shouldBe 5
    }

    it("should render to raw") {
      def toMapString(obj: ConfigObject): Map[String, String] = {
        import scala.collection.JavaConverters._
        obj.entrySet().asScala
          .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
          .toMap
      }

      val e: ContextConfig = ContextConfig(
        name = "foo",
        sparkConf = Map("x.x" -> "y", "z" -> "4"),
        downtime = 10.minutes,
        maxJobs = 10,
        precreated = false,
        runOptions = "",
        workerMode = RunMode.Shared,
        streamingDuration = 1.minutes,
        maxConnFailures = 5
      )
      val raw = ConfigRepr.ContextConfigRepr.toConfig(e)
      Duration(raw.getString("downtime")) shouldBe 10.minutes
      raw.getInt("max-parallel-jobs") shouldBe 10
      raw.getBoolean("precreated") shouldBe false
      val sparkConf = toMapString(raw.getObject("spark-conf"))
      sparkConf.get("x.x") shouldBe Some("y")
      sparkConf.get("z") shouldBe Some("4")

      raw.getString("worker-mode") shouldBe "shared"
      raw.getInt("max-conn-failures") shouldBe 5
      Duration(raw.getString("streaming-duration")) shouldBe 1.minutes
    }
  }

}
