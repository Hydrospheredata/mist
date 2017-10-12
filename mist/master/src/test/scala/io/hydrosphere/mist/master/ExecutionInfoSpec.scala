package io.hydrosphere.mist.master

import mist.api.data._

import io.hydrosphere.mist.core.CommonData.{JobParams, RunJobRequest, Action}
import io.hydrosphere.mist.master.models.JobStartResponse
import org.scalatest._

import scala.concurrent.Promise

class ExecutionInfoSpec extends FunSpec with Matchers {

  import TestUtils._

  val req = RunJobRequest("id", JobParams("path", "class", Map.empty, Action.Execute))

  it("should return job start response") {
    val execInfo = ExecutionInfo(req)
    execInfo.toJobStartResponse shouldBe JobStartResponse("id")
  }

  it("should return jobresult") {
    val promise = Promise[JsLikeData]
    promise.success(JsLikeMap("1" -> JsLikeString("2")))

    val execInfo = ExecutionInfo(req, promise, JobDetails.Status.Finished)

    execInfo.toJobResult.await shouldBe JobResult.success(
      JsLikeMap("1" -> JsLikeString("2"))
    )

  }
}
