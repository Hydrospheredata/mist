package io.hydrosphere.mist.master.interfaces.http

import java.nio.file.Paths
import java.util.UUID

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, RequestEntity, StatusCodes}
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.jar.JobsLoader
import io.hydrosphere.mist.jobs.{Action, JobDetails, JvmJobInfo, PyJobInfo}
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.master.{JobService, MasterService, WorkerLink}
import org.scalatest.{FunSpec, Matchers}
import spray.json.RootJsonWriter

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util._
import org.mockito.Matchers.{anyInt, eq => mockitoEq}
import org.mockito.Mockito.{times, verify}

class HttpApiV2Spec extends FunSpec
  with Matchers
  with MockitoSugar
  with ScalatestRouteTest {

  import JsonCodecs._

  val mappings = new LogStorageMappings(Paths.get("."))

  describe("workers") {

    it("should return workers") {
      val jobService = mock[JobService]
      when(jobService.workers()).thenReturn(Future.successful(Seq(
        WorkerLink("worker", "address", None)
      )))

      val route = HttpV2Routes.workerRoutes(jobService)

      Get("/v2/api/workers") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Seq[WorkerLink]]
        rsp.size shouldBe 1
      }
    }

    it("should stop worker") {
      val jobService = mock[JobService]
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
      val master = mock[MasterService]
      when(master.runJob(any[EndpointStartRequest], any[Source]))
        .thenSuccess(Some(JobStartResponse("1")))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should return bad request on futures failed illegal argument exception") {
      val master = mock[MasterService]

      when(master.runJob(any[EndpointStartRequest], any[Source]))
        .thenFailure(new IllegalArgumentException("argument missing"))

      val route = HttpV2Routes.endpointsRoutes(master)
      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        responseAs[String] shouldBe "Bad request: argument missing"
        status shouldBe StatusCodes.BadRequest
      }
    }

    it("should return 500 on future`s any exception except iae") {
      val master = mock[MasterService]

      when(master.runJob(any[EndpointStartRequest], any[Source]))
        .thenFailure(new IllegalStateException("some exception"))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        status shouldBe StatusCodes.InternalServerError
      }
    }

    it("should return endpoints") {
      val epConfig = EndpointConfig("name", "path", "className", "context")
      val infos = Seq( PyJobInfo, testScalaJob ).map(i => FullEndpointInfo(epConfig, i))

      val master = mock[MasterService]
      when(master.endpointsInfo).thenSuccess(infos)

      val route = HttpV2Routes.endpointsRoutes(master)

      Get("/v2/api/endpoints") ~> route ~> check {
        status shouldBe StatusCodes.OK

        val endpoints = responseAs[Seq[HttpEndpointInfoV2]]
        endpoints.size shouldBe 2
      }
    }

    it("should return history for endpoint") {
      val jobService = mock[JobService]
      val master = mock[MasterService]
      when(master.jobService).thenReturn(jobService)

      when(jobService.endpointHistory(
        any[String], anyInt(), anyInt(), any[Seq[JobDetails.Status]]
      )).thenSuccess(Seq(
          JobDetails("id", "1",
            JobParams("path", "className", Map.empty, Action.Execute),
            "context", None, JobDetails.Source.Http, workerId = "workerId")
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
      val endpointsStorage = mock[EndpointsStorage]
      val master = mock[MasterService]
      when(master.endpoints).thenReturn(endpointsStorage)

      when(endpointsStorage.get(any[String])).thenReturn(Future.successful(None))
      when(endpointsStorage.update(any[EndpointConfig]))
        .thenSuccess(endpointConfig)

      when(master.loadEndpointInfo(any[EndpointConfig])).thenReturn(Success(
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
      val endpointsStorage = mock[EndpointsStorage]
      val master = mock[MasterService]
      when(master.endpoints).thenReturn(endpointsStorage)

      val endpointConfig = EndpointConfig("name", "path", "className", "context")

      when(endpointsStorage.get(any[String])).thenReturn(Future.successful(None))
      when(endpointsStorage.update(any[EndpointConfig]))
        .thenReturn(Future.successful(endpointConfig))
      when(master.loadEndpointInfo(any[EndpointConfig])).thenReturn(Failure(new Exception("test failure")))

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
      val jobsService = mock[JobService]
      val master = mock[MasterService]
      when(master.jobService).thenReturn(jobsService)
      when(jobsService.jobStatusById(any[String]))
        .thenSuccess(Some(jobDetails))

      val route = HttpV2Routes.jobsRoutes(master)

      Get(s"/v2/api/jobs/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }
    it("should return 400 on logs request when job not found") {
      val jobsService = mock[JobService]
      val master = mock[MasterService]
      when(master.jobService).thenReturn(jobsService)
      when(jobsService.jobStatusById(any[String]))
        .thenSuccess(None)

      val route = HttpV2Routes.jobsRoutes(master)
      Get(s"/v2/api/jobs/id/logs") ~> route ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }
    it("should return 200 empty response on logs request when job log file not exists") {
      val jobsService = mock[JobService]
      val master = mock[MasterService]
      val logStorageMappings = mock[LogStorageMappings]
      when(master.jobService).thenReturn(jobsService)
      when(master.logStorageMappings).thenReturn(logStorageMappings)
      when(jobsService.jobStatusById(any[String]))
        .thenSuccess(Some(jobDetails))
      when(logStorageMappings.pathFor(any[String]))
          .thenReturn(Paths.get(".", UUID.randomUUID().toString))
      val route = HttpV2Routes.jobsRoutes(master)
      Get(s"/v2/api/jobs/id/logs") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe ""
      }

    }

  }

  describe("contexts") {

    it("should create jobs with optional parameters") {
      val contextStorage = mock[ContextsStorage]
      val defaultValue = ContextConfig("default", Map.empty, Duration.Inf, 20, precreated = false, "--opt","shared", 1 seconds)
      val contextToCreate = ContextCreateRequest("yoyo", None, None, Some(25), None, None, None, None)

      when(contextStorage.defaultConfig)
        .thenReturn(defaultValue)

      when(contextStorage.get(any[String]))
          .thenSuccess(None)

      when(contextStorage.update(any[ContextConfig]))
        .thenReturn(Future.successful(defaultValue))

      val route = HttpV2Routes.contextsRoutes(contextStorage)

      Post(s"/v2/api/contexts", contextToCreate.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
        verify(contextStorage, times(1)).update(mockitoEq(ContextConfig(
          "yoyo", Map.empty, Duration.Inf, 25, precreated = false, "--opt", "shared", 1 seconds
        )))
      }
    }
  }


  describe("status") {

    it("should return status") {
      val route = HttpV2Routes.statusApi
      Get("/v2/api/status") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[MistStatus]
      }
    }

  }

}
