package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}

trait TestData {

  def mkDetails(status: JobDetails.Status): JobDetails = {
    JobDetails(
      endpoint = "endpoint",
      jobId = "jobId",
      params = JobParams("path", "class", Map("1" -> 2), Action.Execute),
      context = "context",
      externalId = None,
      source = JobDetails.Source.Http,
      status = status,
      workerId = "workerId"
    )
  }


  def mkRunReq(id: String): RunJobRequest = {
    RunJobRequest(id, JobParams("path", "class", Map("1" -> 2), Action.Execute))
  }

  val FooContext = {
    val cfgStr =
      """
        |context-defaults {
        | downtime = Inf
        | streaming-duration = 1 seconds
        | max-parallel-jobs = 20
        | precreated = false
        | spark-conf = { }
        | worker-mode = "shared"
        | run-options = "--opt"
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
      """.stripMargin

    val contextSettings = {
      val cfg = ConfigFactory.parseString(cfgStr)
      ContextsSettings(cfg)
    }
    contextSettings.contexts.get("foo").get
  }

}

object TestData extends TestData
