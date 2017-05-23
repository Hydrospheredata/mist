package io.hydrosphere.mist

import io.hydrosphere.mist.jobs.JobResult
import org.scalatest.{Matchers, FunSpec}

import scalaj.http.Http

import io.hydrosphere.mist.master.interfaces.http.JsonCodecs._
import spray.json.pimpString

class PyJobsSpec extends FunSpec with MistItTest with Matchers {

  override val overrideConf = Some("pyjobs/integration.conf")
  override val overrideRouter = Some("pyjobs/router.conf")

  it("should run simple context") {
    val req = Http("http://localhost:2004/api/simple-context-py")
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
  }

  it("should run hive job") { runOnlyOnSpark1 {
    val req = Http("http://localhost:2004/api/hive-job-py")
      .timeout(60 * 1000, 60 * 1000)
      .header("Content-Type", "application/json")
      .postData(
        s"""
           |{
           |  "path" : "./src/it/resources/pyjobs/jobs/hive_job_data.json"
           |}
         """.stripMargin)

    val resp = req.asString
    resp.code shouldBe 200

    val result = resp.body.parseJson.convertTo[JobResult]

    assert(result.success, s"Job is failed $result")
  }}

  it("should run session hive job") { runOnlyOnSpark2 {
    val req = Http("http://localhost:2004/api/session-py")
      .timeout(60 * 1000, 60 * 1000)
      .header("Content-Type", "application/json")
      .postData(
        s"""
           |{
           |  "path" : "./src/it/resources/pyjobs/jobs/hive_job_data.json"
           |}
         """.stripMargin)

    val resp = req.asString
    resp.code shouldBe 200

    val result = resp.body.parseJson.convertTo[JobResult]

    assert(result.success, s"Job is failed $result")
  }}
}
