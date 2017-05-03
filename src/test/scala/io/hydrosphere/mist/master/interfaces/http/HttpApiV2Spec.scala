package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.jobs.{JobExecutionParams, JobDetails}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.master.models.{JobStartResponse, JobStartRequest}
import org.scalatest.{FunSpec, Matchers}
import org.mockito.Mockito._
import org.mockito.Matchers._

import scala.concurrent.Future

class HttpApiV2Spec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  val jobsPath = "/v2/api/jobs"

  describe("run job") {
    it("should run job") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.runJob(any(classOf[JobStartRequest]), any(classOf[Source])))
        .thenReturn(Future.successful(JobStartResponse("1")))

      Post(s"$jobsPath/x", Map("1" -> "Hello")) ~> api ~> check {
        status === StatusCodes.OK
      }
    }
  }

  describe("status") {

    val jobDetails = JobDetails(
      JobExecutionParams(
        "path", "className", "namespace", Map.empty, Some("externalId"), Some("route")
      ),
      Source.Http, "id"
    )

    it("should return jobs status by id") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.jobStatusById(any(classOf[String])))
        .thenReturn(Future.successful(Some(jobDetails)))

      Get(s"$jobsPath/status/id") ~> api ~> check {
        status === StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }

    it("should return jobs status by external id") {
      val master = mock(classOf[MasterService])
      val api = new HttpApiV2(master).route
      when(master.jobStatusByExternalId(any(classOf[String])))
        .thenReturn(Future.successful(Some(jobDetails)))

      Get(s"$jobsPath/status/id?isExternal=true") ~> api ~> check {
        status === StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }
  }
}
