package io.hydrosphere.mist

import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.jobs.JobResult
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import org.scalatest.{FunSpec, Matchers}

import JsonCodecs._
import spray.json.pimpString

import scalaj.http.Http

class ScalaJobsSpec extends FunSpec with MistItTest  with Matchers {

  override val overrideConf= Some("scalajobs/integration.conf")
  override val overrideRouter = Some("scalajobs/router.conf")

  val interface = MistHttpInterface("localhost", 2004)

  describe("simple context") {
    val sparkPref = sparkVersion.split('.').head

    val jobSource = s"""
       |
       | import io.hydrosphere.mist.api.MistJob
       |
       | object SimpleContext extends MistJob {
       |
       |   def execute(numbers: List[Int], multiplier: Option[Int]): Map[String, Any] = {
       |     val multiplierValue = multiplier.getOrElse(2)
       |     val rdd = context.parallelize(numbers)
       |     Map("result" -> rdd.map(x => x * multiplierValue).collect())
       |   }
       |}
     """.stripMargin
  
    it("should run simple context -spark 1") { runOnlyOnSpark1 {
      val targetDir = "./target/it-jars/simple1"
      val path = Paths.get(targetDir)
      Files.createDirectories(path)
      val compiler = new TestCompiler(targetDir)
      compiler.compile(jobSource, "SimpleContext")
      JarPackager.pack(targetDir, targetDir)

      val result = interface.runJob("simple-context1",
        "numbers" -> List(1, 2, 3)
      )
      assert(result.success, s"Job is failed $result")
    
    }}

    it("should run simple context - spark 2") { runOnlyOnSpark2 {
      val targetDir = "./target/it-jars/simple2"
      val path = Paths.get(targetDir)
      Files.createDirectories(path)
      val compiler = new TestCompiler(targetDir)
      compiler.compile(jobSource, "SimpleContext")
      JarPackager.pack(targetDir, targetDir)

      val result = interface.runJob("simple-context2",
        "numbers" -> List(1, 2, 3)
      )
      assert(result.success, s"Job is failed $result")
    
    }}
  }
}
