package io.hydrosphere.mist

import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.jobs.JobResult
import org.scalatest.{FunSpec, Matchers}

import io.hydrosphere.mist.master.interfaces.http.JsonCodecs._
import spray.json.pimpString

import scalaj.http.Http

class ScalaJobsSpec extends FunSpec with MistItTest  with Matchers {

  override val configPath: String = "scalajobs/integration.conf"

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

      val req = Http("http://localhost:2004/api/simple-context1")
        .timeout(30 * 1000, 30 * 1000)
        .header("Content-Type", "application/json")
        .postData(
          s"""
             |{
             |  "numbers" : [1, 2, 3]
             |}
           """.stripMargin)

      val resp = req.asString
      resp.code shouldBe 200

      val result = resp.body.parseJson.convertTo[JobResult]

      assert(result.success, s"Job is failed $result")
    
    }}

    it("should run simple context - spark 2") { runOnlyOnSpark2 {
      val targetDir = "./target/it-jars/simple2"
      val path = Paths.get(targetDir)
      Files.createDirectories(path)
      val compiler = new TestCompiler(targetDir)
      compiler.compile(jobSource, "SimpleContext")
      JarPackager.pack(targetDir, targetDir)

      val req = Http("http://localhost:2004/api/simple-context2")
        .timeout(30 * 1000, 30 * 1000)
        .header("Content-Type", "application/json")
        .postData(
          s"""
             |{
             |  "numbers" : [1, 2, 3]
             |}
           """.stripMargin)

      val resp = req.asString
      resp.code shouldBe 200

      val result = resp.body.parseJson.convertTo[JobResult]

      assert(result.success, s"Job is failed $result")
    
    }}
  }
}
