package io.hydrosphere.mist.master.interfaces.http

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.ByteString
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.JobInfoData
import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.jobs.JobInfoProviderService
import io.hydrosphere.mist.master.models._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FileUtils
import org.mockito.Matchers.{anyInt, eq => mockitoEq}
import org.mockito.Mockito.{times, verify}
import org.scalatest.{FunSpec, Matchers}
import spray.json.RootJsonWriter

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpApiV2Spec extends FunSpec
  with Matchers
  with MockitoSugar
  with ScalatestRouteTest {

  import JsonCodecs._

  val mappings = new LogStoragePaths(Paths.get("."))

  implicit class ToEntityOps[A](a: A)(implicit f: RootJsonWriter[A]) {
    def toEntity(implicit f: RootJsonWriter[A]): RequestEntity = {
      val data = f.write(a)
      HttpEntity(ContentTypes.`application/json`, data)
    }
  }

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
    it("should get full worker info") {
      val jobService = mock[JobService]
      when(jobService.getWorkerInfo(any[String]))
        .thenSuccess(Some(WorkerFullInfo(
          "id", "test", None, Seq(),
          WorkerInitInfo(Map(), 20, Duration.Inf, Duration.Inf, "test", "localhost:0", "/tmp"))))

      val route = HttpV2Routes.workerRoutes(jobService)

      Get("/v2/api/workers/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkerFullInfo]
        resp.name shouldBe "id"
        resp.jobs shouldBe empty
        resp.initInfo shouldBe WorkerInitInfo(Map(), 20, Duration.Inf, Duration.Inf, "test", "localhost:0", "/tmp")
        resp.sparkUi should not be defined
        resp.address shouldBe "test"
      }
    }

  }

  describe("endpoints") {

    it("should run job") {
      val master = mock[MainService]
      when(master.runJob(any[EndpointStartRequest], any[Source]))
        .thenSuccess(Some(JobStartResponse("1")))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }




    //    it("should return endpoints") {
    //      val epConfig = EndpointConfig("name", "path", "className", "context")
    //      val infos = Seq( PyJobInfo, testScalaJob ).map(i => FullEndpointInfo(epConfig, i))
    //
    //      val master = mock[MainService]
    //      when(master.endpointsInfo).thenSuccess(infos)
    //
    //      val route = HttpV2Routes.endpointsRoutes(master)
    //
    //      Get("/v2/api/endpoints") ~> route ~> check {
    //        status shouldBe StatusCodes.OK
    //
    //        val endpoints = responseAs[Seq[HttpEndpointInfoV2]]
    //        endpoints.size shouldBe 2
    //      }
    //    }

    it("should return history for endpoint") {
      val jobService = mock[JobService]
      val master = mock[MainService]
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

    it("should update endpoint on create if endpoint created") {

      val endpoints = mock[EndpointsStorage]
      val jobInfoProvider = mock[JobInfoProviderService]

      val mainService = new MainService(
        mock[JobService],
        endpoints,
        mock[ContextsStorage],
        mock[LogStoragePaths],
        jobInfoProvider
      )

      val test = EndpointConfig("test", "test", "test", "default")

      when(endpoints.get(any[String]))
        .thenSuccess(None)

      when(endpoints.update(any[EndpointConfig]))
        .thenSuccess(test)

      when(jobInfoProvider.getJobInfoByConfig(any[EndpointConfig]))
        .thenSuccess(JobInfoData(
          lang = "python",
          path = "test",
          defaultContext = "foo",
          className = "test",
          name = "test"
        ))

      val route = HttpV2Routes.endpointsRoutes(mainService)

      Post("/v2/api/endpoints", test.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
        verify(endpoints, times(1)).update(any[EndpointConfig])
      }
    }

    it("should return different entity when forcibly update") {
      val endpoints = mock[EndpointsStorage]
      val master = new MainService(
        mock[JobService],
        endpoints,
        mock[ContextsStorage],
        mock[LogStoragePaths],
        mock[JobInfoProviderService]
      )
      val test = EndpointConfig("test", "test", "test", "default")
      when(endpoints.update(any[EndpointConfig]))
        .thenSuccess(test)

      val route = HttpV2Routes.endpointsRoutes(master)

      Post("/v2/api/endpoints?force=true", test.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
        verify(endpoints, times(1)).update(any[EndpointConfig])
        responseAs[EndpointConfig] shouldBe test
      }
    }

    it("should fail with invalid data for endpoint") {
      val endpointsStorage = mock[EndpointsStorage]
      val master = mock[MainService]
      val jobInfoProvider = mock[JobInfoProviderService]
      when(master.endpoints).thenReturn(endpointsStorage)
      when(master.jobInfoProviderService).thenReturn(jobInfoProvider)

      val endpointConfig = EndpointConfig("name", "path", "className", "context")

      when(endpointsStorage.get(any[String])).thenReturn(Future.successful(None))
      when(endpointsStorage.update(any[EndpointConfig]))
        .thenReturn(Future.successful(endpointConfig))
      when(jobInfoProvider.getJobInfoByConfig(any[EndpointConfig]))
        .thenFailure(new Exception("test failure"))

      val route = HttpV2Routes.endpointsRoutes(master)

      Post("/v2/api/endpoints", endpointConfig.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
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
      externalId = None,
      workerId = "workerId"
    )

    it("should return jobs status by id") {
      val jobsService = mock[JobService]
      val master = mock[MainService]
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
      val master = mock[MainService]
      when(master.jobService).thenReturn(jobsService)
      when(jobsService.jobStatusById(any[String]))
        .thenSuccess(None)

      val route = HttpV2Routes.jobsRoutes(master)
      Get(s"/v2/api/jobs/id/logs") ~> route ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }
    it("should return worker info") {
      val jobService = mock[JobService]
      val master = mock[MainService]
      when(master.jobService)
        .thenReturn(jobService)
      when(jobService.workerByJobId(any[String]))
        .thenSuccess(Some(WorkerLink("test", "localhost:0", None)))

      val route = HttpV2Routes.jobsRoutes(master)

      Get("/v2/api/jobs/id/worker") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[WorkerLink].name shouldBe "test"
      }
    }
    it("should return 404 when worker not found") {
      val jobService = mock[JobService]
      val master = mock[MainService]
      when(master.jobService)
        .thenReturn(jobService)
      when(jobService.workerByJobId(any[String]))
        .thenSuccess(None)
      val route = HttpV2Routes.jobsRoutes(master)

      Get("/v2/api/jobs/id/worker") ~> route ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }
    it("should return 200 empty response on logs request when job log file not exists") {
      val jobsService = mock[JobService]
      val master = mock[MainService]
      val logStorageMappings = mock[LogStoragePaths]
      when(master.jobService).thenReturn(jobsService)
      when(master.logsPaths).thenReturn(logStorageMappings)
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
      val defaultValue = ContextConfig("default", Map.empty, Duration.Inf, 20, precreated = false, "--opt", "shared", 1 seconds)
      val contextToCreate = ContextCreateRequest("yoyo", None, None, Some(25), None, None, None, None)

      when(contextStorage.defaultConfig)
        .thenReturn(defaultValue)

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
  describe("artifact") {
    it("should list all unique file names in artifact repository") {
      val artifactRepo = mock[ArtifactRepository]
      when(artifactRepo.listPaths()).thenSuccess(Set("test.jar", "test.py"))

      val routes = HttpV2Routes.artifactRoutes(artifactRepo)

      Get("/v2/api/artifacts") ~> routes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[Set[String]] should contain allOf("test.jar", "test.py")
      }
    }

    it("should upload file if it unique") {
      val artifactRepo = mock[ArtifactRepository]
      when(artifactRepo.get(any[String]))
        .thenReturn(None)
      when(artifactRepo.store(any[File], any[String]))
        .thenSuccess(new File("some/internal/path/test.jar"))

      val routes = HttpV2Routes.artifactRoutes(artifactRepo)
      val multipartForm =
        Multipart.FormData(Multipart.FormData.BodyPart.Strict(
          "file",
          HttpEntity(ContentTypes.`application/octet-stream`, ByteString.fromString("Jar content")),
          Map("filename" -> "test.jar")))

      Post("/v2/api/artifacts", multipartForm) ~> routes ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe "test.jar"
      }
    }

    it("should return 400 when upload filename not unique") {
      val artifactRepo = mock[ArtifactRepository]
      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File("test.jar")))

      val routes = HttpV2Routes.artifactRoutes(artifactRepo)
      val multipartForm =
        Multipart.FormData(Multipart.FormData.BodyPart.Strict(
          "file",
          HttpEntity(ContentTypes.`application/octet-stream`, ByteString.fromString("Jar content")),
          Map("filename" -> "test.jar")))

      Post("/v2/api/artifacts", multipartForm) ~> routes ~> check {
        status shouldBe StatusCodes.Conflict
      }
    }
    it("should not check uniqueness when force flag applied") {
      val artifactRepo = mock[ArtifactRepository]
      val routes = HttpV2Routes.artifactRoutes(artifactRepo)
      when(artifactRepo.store(any[File], any[String]))
        .thenSuccess(new File("some/internal/path/test.jar"))
      val multipartForm =
        Multipart.FormData(Multipart.FormData.BodyPart.Strict(
          "file",
          HttpEntity(ContentTypes.`application/octet-stream`, ByteString.fromString("Jar content")),
          Map("filename" -> "test.jar")))

      Post("/v2/api/artifacts?force=true", multipartForm) ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should download file if it exists") {
      val artifactRepo = mock[ArtifactRepository]
      val file = new File("./target/test.jar")
      FileUtils.touch(file)
      when(artifactRepo.get(any[String]))
        .thenReturn(Some(file))

      val routes = HttpV2Routes.artifactRoutes(artifactRepo)


      Get("/v2/api/artifacts/test.jar") ~> routes ~> check {
        status shouldBe StatusCodes.OK
      }

      FileUtils.deleteQuietly(file)
    }

    it("should return not found when download file not exists") {

      val artifactRepo = mock[ArtifactRepository]
      when(artifactRepo.get(any[String]))
        .thenReturn(None)
      val routes = HttpV2Routes.artifactRoutes(artifactRepo)


      Get("/v2/api/artifacts/test.jar") ~> routes ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }

    it("should return sha of given filename") {
      val file = new File("./target/test.jar")
      FileUtils.touch(file)

      val expectedHex = DigestUtils.sha1Hex(Files.newInputStream(file.toPath))

      val artifactRepository = mock[ArtifactRepository]
      when(artifactRepository.get(any[String]))
        .thenReturn(Some(file))

      val routes = HttpV2Routes.artifactRoutes(artifactRepository)
      Get("/v2/api/artifacts/test.jar/sha") ~> routes ~> check {
        responseAs[String] shouldBe expectedHex
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

  describe("full api") {

    it("should return bad request on futures failed illegal argument exception") {
      val master = mock[MainService]

      when(master.runJob(any[EndpointStartRequest], any[Source]))
        .thenFailure(new IllegalArgumentException("argument missing"))

      val route = HttpV2Routes.apiRoutes(master, mock[ArtifactRepository])
      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        responseAs[String] shouldBe "Bad request: argument missing"
        status shouldBe StatusCodes.BadRequest
      }
    }

    it("should return 500 on future`s any exception except iae") {
      val master = mock[MainService]

      when(master.runJob(any[EndpointStartRequest], any[Source]))
        .thenFailure(new RuntimeException("some exception"))

      val route = HttpV2Routes.apiRoutes(master, mock[ArtifactRepository])

      Post(s"/v2/api/endpoints/x/jobs", Map("1" -> "Hello")) ~> route ~> check {
        status shouldBe StatusCodes.InternalServerError
      }
    }
  }

}
