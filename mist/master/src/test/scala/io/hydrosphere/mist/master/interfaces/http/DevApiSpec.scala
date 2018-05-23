package io.hydrosphere.mist.master.interfaces.http

import mist.api.data._
import mist.api.encoding.JsSyntax._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.core.CommonData.{JobParams, RunJobRequest}
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.CommonData.Action
import io.hydrosphere.mist.master.JobDetails.{Source, Status}
import io.hydrosphere.mist.master.execution.ExecutionInfo
import io.hydrosphere.mist.master.MainService
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{DevJobStartRequest, DevJobStartRequestModel}
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Promise

class DevApiSpec extends FunSpec with Matchers with ScalatestRouteTest with MockitoSugar {

  import JsonCodecs._
  import io.hydrosphere.mist.master.TestUtils._

  it("should start job in dev mode") {
    val master = mock[MainService]
    val api = DevApi.devRoutes(master)

    val req = DevJobStartRequestModel("simple-context", "path", "className", None, None, None, "foo")

    val promise = Promise[JsData]
    promise.success(JsUnit)

    when(master.devRun(any[DevJobStartRequest], any[Source], any[Action]))
      .thenSuccess(ExecutionInfo(
        RunJobRequest("id", JobParams("path", "className", JsMap.empty, Action.Execute)),
        promise
      ))

    Post("/v2/hidden/devrun", req) ~> api ~> check {
      status shouldBe StatusCodes.OK
    }
  }
}
