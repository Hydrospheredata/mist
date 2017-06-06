package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{JobDefinition, PyJobInfo, Action, JobDetails}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.{WorkerLink, MasterService}
import io.hydrosphere.mist.master.models.{JobStartRequest, JobStartResponse}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpApiV2Spec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  describe("endpoint") {

    val pyInfo = PyJobInfo(JobDefinition("x", "path", "class", "namespace"))

    it("should run job") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.endpointInfo(any(classOf[String]))).thenReturn(Some(pyInfo))

      when(master.runJob(any(classOf[JobStartRequest]), any(classOf[Source]), any[Action]))
        .thenReturn(Future.successful(JobStartResponse("1")))

      Post(s"/v2/api/endpoints/x", Map("1" -> "Hello")) ~> api ~> check {
        status === StatusCodes.OK
      }
    }

    it("should return endpoints") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.listEndpoints()).thenReturn(Seq(pyInfo))

      Get("/v2/api/endpoints") ~> api ~> check {
        status === StatusCodes.OK

        val endpoints = responseAs[Seq[HttpEndpointInfoV2]]
        endpoints.size shouldBe 1
        endpoints.head.name shouldBe "x"
      }
    }

    it("should return history for endpoint") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route

      when(master.endpointHistory(any(classOf[String]), any(classOf[Int]), any(classOf[Int])))
        .thenReturn(Future.successful(
          Seq(
            JobDetails("id", "1",
              JobParams("path", "className", Map.empty, Action.Execute),
              "context", None, JobDetails.Source.Http)
          )
        ))

      Get("/v2/api/endpoints/id/jobs") ~> api ~> check {
        status === StatusCodes.OK

        val jobs = responseAs[Seq[JobDetails]]
        jobs.size shouldBe 1
      }
    }
  }

  describe("jobs") {

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

  describe("workers") {

    it("should return workers") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.workers()).thenReturn(Future.successful(Seq(
        WorkerLink("worker", "address")
      )))

      Get("/v2/api/workers") ~> api ~> check {
        status === StatusCodes.OK
        val rsp = responseAs[Seq[WorkerLink]]
        rsp.size shouldBe 1
      }
    }

    it("should stop worker") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route

      when(master.stopWorker(any[String])).thenReturn(Future.successful("id"))

      Delete("/v2/api/workers/id") ~> api ~> check {
        status === StatusCodes.OK
      }
    }

  }
}
