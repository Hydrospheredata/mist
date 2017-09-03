package io.hydrosphere.mist

import org.scalatest._

class PyJobsSpec extends FunSpec with MistItTest with Matchers {

  override val overrideConf = Some("pyjobs/integration.conf")
  override val overrideRouter = Some("pyjobs/router.conf")

  val interface = MistHttpInterface("localhost", 2004)

  it("should run simple context") {
    val result = interface.runJob("simple-context-py",
      "numbers" -> List(1, 2, 3)
    )
    assert(result.success, s"Job is failed $result")
  }

  it("should run hive job") { runOnlyOnSpark1 {
    val result = interface.runJob("hive-job-py",
      "path" -> "./src/it/resources/pyjobs/jobs/hive_job_data.json"
    )
    assert(result.success, s"Job is failed $result")
  }}

  it("should run session hive job") { runOnlyOnSpark2 {
    val result = interface.runJob("session-py",
      "path" -> "./src/it/resources/pyjobs/jobs/hive_job_data.json"
    )
    assert(result.success, s"Job is failed $result")
  }}
}
