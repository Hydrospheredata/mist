package io.hydrosphere.mist.master.interfaces.http

import java.nio.file.Paths

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, RequestEntity, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.jar.JobsLoader
import io.hydrosphere.mist.jobs.{Action, JobDetails, JvmJobInfo, PyJobInfo}
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models.{EndpointConfig, FullEndpointInfo, JobStartRequest, JobStartResponse}
import io.hydrosphere.mist.master.{JobService, MasterService, WorkerLink}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}
import spray.json.RootJsonWriter

import scala.concurrent.Future
import scala.util._

class HttpApiV2Spec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  val mappings = new LogStorageMappings(Paths.get("."))

  describe("workers") {

    it("should return workers") {
      val jobService = mock(classOf[JobService])
      when(jobService.workers()).thenReturn(Future.successful(Seq(
        WorkerLink("worker", "address")
      )))

      val route = HttpV2Routes.workerRoutes(jobService)

      Get("/v2/api/workers") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Seq[WorkerLink]]
        rsp.size shouldBe 1
      }
    }

    it("should stop worker") {
      val jobService = mock(classOf[JobService])
      when(jobService.stopWorker(any[String])).thenReturn(Future.successful(()))

      val route = HttpV2Routes.workerRoutes(jobService)

      Delete("/v2/api/workers/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

  }

  val scalaJobClass = io.hydrosphere.mist.jobs.jar.MultiplyJob.getClass
  val testScalaJob = JvmJobInfo(JobsLoader.Common.loadJobClass(scalaJobClass.getName).get)

  describe("endpoints") {

    it("should run job") {
      val master = mock(classOf[MasterService])
      when(master.runJob(any(classOf[JobStartRequest]), any(classOf[Source])))
        .thenReturn(Future.successful(Some(JobStartResponse("1"))))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should return bad request on futures failed illegal argument exception") {
      val master = mock(classOf[MasterService])

      when(master.runJob(any(classOf[JobStartRequest]), any(classOf[Source])))
        .thenReturn(Future.failed(new IllegalArgumentException("argument missing")))

      val route = HttpV2Routes.endpointsRoutes(master)
      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        responseAs[String] shouldBe "Bad request: argument missing"
        status shouldBe StatusCodes.BadRequest
      }
    }

    it("should return 500 on future`s any exception except iae") {
      val master = mock(classOf[MasterService])

      when(master.runJob(any(classOf[JobStartRequest]), any(classOf[Source])))
        .thenReturn(Future.failed(new IllegalStateException("some exception")))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        status shouldBe StatusCodes.InternalServerError
      }
    }

    it("should return endpoints") {
      val epConfig = EndpointConfig("name", "path", "className", "context")
      val infos = Seq( PyJobInfo, testScalaJob ).map(i => FullEndpointInfo(epConfig, i))

      val master = mock(classOf[MasterService])
      when(master.endpointsInfo).thenReturn(Future.successful(infos))

      val route = HttpV2Routes.endpointsRoutes(master)

      Get("/v2/api/endpoints") ~> route ~> check {
        status shouldBe StatusCodes.OK

        val endpoints = responseAs[Seq[HttpEndpointInfoV2]]
        endpoints.size shouldBe 2
      }
    }

    it("should return history for endpoint") {
      val jobService = mock(classOf[JobService])
      val master = mock(classOf[MasterService])
      when(master.jobService).thenReturn(jobService)

      when(jobService.endpointHistory(
        any(classOf[String]), any(classOf[Int]), any(classOf[Int]), any(classOf[Seq[JobDetails.Status]])
      )).thenReturn(Future.successful(
        Seq(
          JobDetails("id", "1",
            JobParams("path", "className", Map.empty, Action.Execute),
            "context", None, JobDetails.Source.Http, workerId = "workerId")
        )
      ))

      val route = HttpV2Routes.endpointsRoutes(master)

      Get("/v2/api/endpoints/id/jobs?status=started") ~> route ~> check {
        status shouldBe StatusCodes.OK

        val jobs = responseAs[Seq[JobDetails]]
        jobs.size shouldBe 1
      }
    }

  }

  describe("endpoint creation") {

    val endpointConfig = EndpointConfig("name", "path", "className", "context")

    it("should create endpoint") {
      val endpointsStorage = mock(classOf[EndpointsStorage])
      val master = mock(classOf[MasterService])
      when(master.endpoints).thenReturn(endpointsStorage)

      when(endpointsStorage.get(any(classOf[String]))).thenReturn(Future.successful(None))
      when(endpointsStorage.update(any(classOf[EndpointConfig])))
        .thenReturn(Future.successful(endpointConfig))
      when(master.loadEndpointInfo(any(classOf[EndpointConfig]))).thenReturn(Success(
        FullEndpointInfo(endpointConfig, testScalaJob)
      ))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post("/v2/api/endpoints", endpointConfig.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
        val info = responseAs[HttpEndpointInfoV2]
        info.name shouldBe "name"
      }
    }

    it("should fail with invalid data for endpoint") {
      val endpointsStorage = mock(classOf[EndpointsStorage])
      val master = mock(classOf[MasterService])
      when(master.endpoints).thenReturn(endpointsStorage)

      val endpointConfig = EndpointConfig("name", "path", "className", "context")

      when(endpointsStorage.get(any(classOf[String]))).thenReturn(Future.successful(None))
      when(endpointsStorage.update(any(classOf[EndpointConfig])))
        .thenReturn(Future.successful(endpointConfig))
      when(master.loadEndpointInfo(any(classOf[EndpointConfig]))).thenReturn(Failure(new Exception("test failure")))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post("/v2/api/endpoints", endpointConfig.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }

    }
  }


  implicit class ToEntityOps[A](a: A)(implicit f: RootJsonWriter[A]) {
    def toEntity(implicit f: RootJsonWriter[A]): RequestEntity =  {
      val data = f.write(a)
      HttpEntity(ContentTypes.`application/json`, data)
    }
  }

  describe("jobs") {

    val jobDetails = JobDetails(
      params = JobParams("path", "className", Map.empty, Action.Execute),
      jobId = "id",
      source = Source.Http,
      endpoint = "endpoint",
      context = "context",
      externalId = None,
      workerId = "workerId"
    )

    it("should return jobs status by id") {
      val jobsService = mock(classOf[JobService])
      val master = mock(classOf[MasterService])
      when(master.jobService).thenReturn(jobsService)
      when(jobsService.jobStatusById(any(classOf[String]))).thenReturn(Future.successful(
        Some(jobDetails)
      ))

      val route = HttpV2Routes.jobsRoutes(master)

      Get(s"/v2/api/jobs/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }

  }

}
