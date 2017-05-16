package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{JobDefinition, PyJobInfo, Action, JobDetails}
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.{JobStartRequest, JobStartResponse}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpApiV2Spec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  val endpointsPath = "/v2/api/endpoints"

  describe("run job") {
    it("should run job") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.endpointInfo(any(classOf[String])))
        .thenReturn(Some(PyJobInfo(JobDefinition("x", "path", "class", "namespace"))))

      when(master.runJob(any(classOf[JobStartRequest]), any(classOf[Source]), any[Action]))
        .thenReturn(Future.successful(JobStartResponse("1")))

      Post(s"/v2/api/endpoints/x", Map("1" -> "Hello")) ~> api ~> check {
        status === StatusCodes.OK
      }
    }
  }

  describe("status") {

    val jobDetails = JobDetails(
      params = JobParams("path", "className", Map.empty, Action.Execute),
      jobId = "id",
      source = Source.Http,
      endpoint = "endpoint",
      context = "context",
      externalId = None
    )

    it("should return jobs status by id") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.jobStatusById(any(classOf[String])))
        .thenReturn(Future.successful(Some(jobDetails)))

      Get(s"/v2/api/jobs/id") ~> api ~> check {
        status === StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }
  }
}
