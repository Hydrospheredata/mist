package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.jobs.Action
import io.hydrosphere.mist.jobs.JobDetails.{Status, Source}
import io.hydrosphere.mist.master.{ExecutionInfo, MasterService}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{DevJobStartRequest, DevJobStartRequestModel}
import org.scalatest.{Matchers, FunSpec}

import scala.concurrent.Promise

class DevApiSpec extends FunSpec with Matchers with ScalatestRouteTest with MockitoSugar {

  import JsonCodecs._
  import io.hydrosphere.mist.master.TestUtils._

  it("should start job in dev mode") {
    val master = mock[MasterService]
    val api = DevApi.devRoutes(master)

    val req = DevJobStartRequestModel("simple-context", "path", "className", None, None, None, "foo")

    val promise = Promise[Map[String, Any]]
    promise.success(Map.empty)

    when(master.devRun(any[DevJobStartRequest], any[Source], any[Action]))
      .thenSuccess(ExecutionInfo(
        RunJobRequest("id", JobParams("path", "className", Map.empty, Action.Execute)),
        promise,
        Status.Finished
      ))

    Post("/v2/hidden/devrun", req) ~> api ~> check {
      status shouldBe StatusCodes.OK
    }
  }
}
