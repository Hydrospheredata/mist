package io.hydrosphere.mist.master.data

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.master.models.{ContextConfig, EndpointConfig}
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class ConfigReprsSpec extends FunSpec with Matchers {

  describe("EndpointConfig") {

    it("should be parsed from config") {
      val cfg = ConfigFactory.parseString(
        """
         |  path = "jar_path.jar"
         |  className = "MyJob1"
         |  namespace = "namespace"
      """.stripMargin)

      val parsed = ConfigRepr.EndpointsRepr.fromConfig("name", cfg)
      parsed shouldBe EndpointConfig("name", "jar_path.jar", "MyJob1", "namespace")
    }

    it("should render to raw") {
      val e = EndpointConfig("name", "jar_path.jar", "MyJob1", "namespace")
      val raw = ConfigRepr.EndpointsRepr.toConfig(e)

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
          |    x = "z"
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
      )

      val context = ConfigRepr.ContextConfigRepr.fromConfig("test", cfg)
      context.sparkConf shouldBe Map("x" -> "z", "a" -> "1")
      context.downtime shouldBe Duration.Inf
      context.maxJobs shouldBe 100
      context.precreated shouldBe false
      context.runOptions shouldBe "--key"
      context.workerMode shouldBe "shared"
      context.streamingDuration shouldBe 30.seconds
    }

    it("should render to raw") {
      val context = ContextConfig(
        name = "foo",
        sparkConf = Map("x" -> "y", "z" -> "4"),
        downtime = 10.minutes,
        maxJobs = 10,
        precreated = false,
        runOptions = "",
        workerMode = "shared",
        streamingDuration = 1.minutes
      )
      val raw = ConfigRepr.ContextConfigRepr.toConfig(context)
      Duration(raw.getString("downtime")) shouldBe 10.minutes
      raw.getInt("max-parallel-jobs") shouldBe 10
      raw.getBoolean("precreated") shouldBe false
      raw.getString("spark-conf.x") shouldBe "y"
      raw.getString("spark-conf.z") shouldBe "4"
      raw.getString("worker-mode") shouldBe "shared"
      Duration(raw.getString("streaming-duration")) shouldBe 1.minutes
    }
  }

}
