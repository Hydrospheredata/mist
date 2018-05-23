package io.hydrosphere.mist.master

import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest, WorkerInitInfo}
import io.hydrosphere.mist.core.FunctionInfoData
import io.hydrosphere.mist.master.execution.WorkerLink
import mist.api.ArgInfo
import mist.api.data._
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._

trait TestData {

  def mkDetails(status: JobDetails.Status): JobDetails = {
    JobDetails(
      function = "function",
      jobId = "jobId",
      params = JobParams("path", "class", JsMap("1" -> 2.js), Action.Execute),
      context = "context",
      externalId = None,
      source = JobDetails.Source.Http,
      status = status
    )
  }


  def mkRunReq(id: String): RunJobRequest = {
    RunJobRequest(id, JobParams("path", "class", JsMap("1" -> 2.js), Action.Execute))
  }

  val FooContext = {
    val cfgStr =
      """
        |context-defaults {
        | downtime = Inf
        | streaming-duration = 1 seconds
        | max-parallel-jobs = 2
        | precreated = false
        | spark-conf = { }
        | worker-mode = "shared"
        | run-options = "--opt"
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
      """.stripMargin

    val contextSettings = {
      val cfg = ConfigFactory.parseString(cfgStr)
      ContextsSettings(cfg)
    }
    contextSettings.contexts.get("foo").get
  }

  val functionInfoData = FunctionInfoData(
    "test",
    "test",
    "Test",
    "foo",
    FunctionInfoData.PythonLang,
    tags = Seq(ArgInfo.SqlContextTag)
  )

  val workerInitData = WorkerInitInfo(
    sparkConf = FooContext.sparkConf,
    maxJobs = FooContext.maxJobs,
    downtime = FooContext.downtime,
    streamingDuration = FooContext.streamingDuration,
    logService = "localhost:2005",
    masterAddress = "localhost:2003",
    masterHttpConf = "localhost:2004",
    maxArtifactSize = 1000L,
    runOptions = ""
  )

  val workerLinkData = WorkerLink("worker", "address", None, workerInitData)

}

object TestData extends TestData
