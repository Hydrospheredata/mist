package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.jar._
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{FullEndpointInfo, EndpointConfig, EndpointStartRequest, RunSettings}
import io.hydrosphere.mist.master.{JobService, MasterService, WorkerLink}
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpApiSpec extends FunSpec with Matchers with MockitoSugar with ScalatestRouteTest {

  import JsonCodecs._

  val details = JobDetails(
    params = JobParams("path", "className", Map.empty, Action.Execute),
    jobId = "id",
    source = Source.Http,
    endpoint = "endpoint",
    context = "context",
    externalId = None,
    workerId = "workerId"
  )

  it("should serve active jobs") {
    val jobService = mock[JobService]
    when(jobService.activeJobs()).thenReturn(
      Future.successful(List(details))
    )

    val api = new HttpApi(masterMock(jobService)).route

    Get("/internal/jobs") ~> api ~> check {
      status shouldBe StatusCodes.OK

      val r = responseAs[List[JobDetails]]
      r.size shouldBe 1
    }
  }

  it("should stop job") {
    val jobService = mock[JobService]
    when(jobService.stopJob(any[String])).thenReturn(Future.successful(Some(details)))

    val api = new HttpApi(masterMock(jobService)).route

    Delete("/internal/jobs/namespace/id") ~> api ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  it("should serve workers") {
    val jobService = mock[JobService]
    when(jobService.workers()).thenReturn(
      Future.successful( List(WorkerLink("name", "address", None)) ))

    val api = new HttpApi(masterMock(jobService)).route

    Get("/internal/workers") ~> api ~> check {
      status shouldBe StatusCodes.OK

      val r = responseAs[List[WorkerLink]]
      r shouldBe List(WorkerLink("name", "address", None))
    }
  }

  it("should stop worker") {
    val jobService = mock[JobService]
    when(jobService.stopWorker(any[String])).thenReturn(Future.successful(()))

    val api = new HttpApi(masterMock(jobService)).route

    Delete("/internal/workers/id") ~> api ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  it("should stop workers") {
    val jobService = mock[JobService]
    when(jobService.stopAllWorkers()).thenReturn(Future.successful(()))

    val api = new HttpApi(masterMock(jobService)).route
    Delete("/internal/workers") ~> api ~> check {
      status shouldBe StatusCodes.OK
    }
  }


  it("should serve routes") {
    val master = mock[MasterService]
    val api = new HttpApi(master).route

    val testJobClass = io.hydrosphere.mist.jobs.jar.MultiplyJob
    val jvmInfo = JvmJobInfo(JobsLoader.Common.loadJobClass(testJobClass.getClass.getCanonicalName).get)

    when(master.endpointsInfo).thenReturn(Future.successful(
      Seq(
      FullEndpointInfo(EndpointConfig("pyjob", "", "", ""), PyJobInfo),
      FullEndpointInfo(EndpointConfig("scalajob", "", "", ""), jvmInfo)
    )))

    Get("/internal/routers") ~> api ~> check {
      status shouldBe StatusCodes.OK

      responseAs[Map[String, HttpJobInfo]] should contain allOf(
        "pyjob" -> HttpJobInfo.forPython("pyjob"),
        "scalajob" -> HttpJobInfo(
          name = "scalajob",
          execute = Some(
            Map("numbers" -> HttpJobArg("MList", Seq(HttpJobArg("MInt", Seq.empty))))
          )
        )
      )
    }
  }

  it("should start job") {
    val master = mock[MasterService]
    val api = new HttpApi(master).route
    when(master.forceJobRun(any[EndpointStartRequest], any[Source], any[Action]))
      .thenReturn(Future.successful(Some(
        JobResult.success(Map("yoyo" -> "hello"))
      )))

    Post("/api/my-job", Map("Hello" -> "123")) ~> api ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  def masterMock(jobService: JobService): MasterService = {
    val master = mock[MasterService]
    when(master.jobService).thenReturn(jobService)
    master
  }
}
