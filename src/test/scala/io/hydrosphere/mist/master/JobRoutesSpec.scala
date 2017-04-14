package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.jobs.{JobDefinition, JobConfiguration}
import org.scalatest.{Matchers, FunSpec}

import scala.util._

class JobRoutesSpec extends FunSpec with Matchers {

  describe("Job Configuration") {

    it("should be parsed from config") {
      val cfg = ConfigFactory.parseString(
        """
         |  path = "jar_path.jar"
         |  className = "MyJob1"
         |  namespace = "namespace"
      """.stripMargin)

      val parsed = JobConfiguration.fromConfig(cfg)
      parsed shouldBe Success(JobConfiguration("jar_path.jar", "MyJob1", "namespace"))
    }

    it("should be failed with no params") {
      val failure = JobConfiguration.fromConfig(ConfigFactory.empty())
      failure.isFailure shouldBe true
    }

    it("should be failed with empty params") {
      val cfg = ConfigFactory.parseString(
        """
          |  path = "jar_path.jar"
          |  className = null
          |  namespace = "namespace"
        """.stripMargin)
      val failure = JobConfiguration.fromConfig(cfg)
      failure.isFailure shouldBe true
    }
  }

  describe("job definition") {

    it("should parse jobs from config") {
      val cfg = ConfigFactory.parseString(
        """
          |my-job {
          |  path = "jar_path.jar"
          |  className = "MyJob"
          |  namespace = "namespace"
          |}
        """.stripMargin
      )

      val parsed = JobDefinition.parseConfig(cfg)
      parsed.size shouldBe 1

      val result = parsed.head
      result.isSuccess shouldBe true
      val job = result.get
      job shouldBe JobDefinition("my-job", "jar_path.jar", "MyJob", "namespace")
    }

    it("should parse more than one jobs") {
      val cfg = ConfigFactory.parseString(
        """
          |my-job1 {
          |  path = "jar_path.jar"
          |  className = "MyJob"
          |  namespace = "namespace"
          |}
          |
          |my-job2 {
          |  path = "jar_path.jar"
          |  className = "MyJob2"
          |  namespace = "namespace"
          |}
        """.stripMargin
      )

      val jobs = JobDefinition.parseConfig(cfg).flatMap(_.toOption)
      jobs.size shouldBe 2
      jobs.map(_.name) should contain allOf ("my-job1", "my-job2")
    }

    it("should use variables") {
      val cfg = ConfigFactory.parseString(
        s"""
          |dir = "root_path"
          |my-job {
          |  path = $${dir}"/jar_path.jar"
          |  className = "MyJob"
          |  namespace = "namespace"
          |}
        """.stripMargin
      ).resolve()

      val jobs = JobDefinition.parseConfig(cfg).flatMap(_.toOption)
      jobs.size shouldBe 1
      jobs.head.path shouldBe "root_path/jar_path.jar"
    }
  }


}
