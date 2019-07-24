package io.hydrosphere.mist.master.interfaces.http

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.UUID

import akka.http.scaladsl.model._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.ByteString
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.{FunctionInfoData, MockitoSugar}
import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master._
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.execution.{ExecutionService, WorkerLink}
import io.hydrosphere.mist.master.data.{ContextsStorage, FunctionConfigStorage}
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.jobs.FunctionsService
import io.hydrosphere.mist.master.models._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FileUtils
import org.mockito.Matchers.{anyInt, eq => mockitoEq}
import org.mockito.Mockito.{times, verify}
import org.scalatest.{FunSpec, Matchers}
import spray.json.RootJsonWriter
import mist.api.data._
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._

import scala.concurrent.Future
import scala.concurrent.duration._

class HttpApiV2Spec extends FunSpec
  with Matchers
  with MockitoSugar
  with ScalatestRouteTest
  with TestData
  with TestUtils {

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
      val execution = mock[ExecutionService]
      when(execution.workers()).thenReturn(Seq(workerLinkData))

      val route = HttpV2Routes.workerRoutes(execution)

      Get("/v2/api/workers") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Seq[WorkerLink]]
        rsp.size shouldBe 1
      }
    }

    it("should stop worker") {
      val execution = mock[ExecutionService]
      when(execution.stopWorker(any[String])).thenSuccess(())

      val route = HttpV2Routes.workerRoutes(execution)

      Delete("/v2/api/workers/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should get full worker info") {
      val execution = mock[ExecutionService]
      when(execution.getWorkerLink(any[String]))
        .thenReturn(Some(WorkerLink(
          "id", "test", None,
          WorkerInitInfo(Map(), 20, Duration.Inf, Duration.Inf, "test", "localhost:0", "localhost:0", 262144000, ""))))

      val route = HttpV2Routes.workerRoutes(execution)

      Get("/v2/api/workers/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val resp = responseAs[WorkerLink]
        resp.name shouldBe "id"
        resp.initInfo shouldBe WorkerInitInfo(Map(), 20, Duration.Inf, Duration.Inf, "test", "localhost:0","localhost:0", 262144000, "")
        resp.sparkUi should not be defined
        resp.address shouldBe "test"
      }
    }

    it("should return worker jobs") {
      val execution = mock[ExecutionService]
      when(execution.getHistory(any[JobDetailsRequest])).thenSuccess(JobDetailsResponse(
        Seq(JobDetails("id", "1",
          JobParams("path", "className", JsMap.empty, Action.Execute),
          "context", None, JobDetails.Source.Http)
        ), 1
      ))

      val route = HttpV2Routes.workerRoutes(execution)

      Get("/v2/api/workers/id/jobs?status=started") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val jobs = responseAs[Seq[JobDetails]]
        jobs.size shouldBe 1
      }
      Get("/v2/api/workers/id/jobs?status=started&paginate=true") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[JobDetailsResponse]
        rsp.jobs.size shouldBe 1
      }
    }

  }

  describe("functions") {

    it("should run job") {
      val master = mock[MainService]
      when(master.runJob(any[FunctionStartRequest], any[Source]))
        .thenSuccess(Some(JobStartResponse("1")))

      val route = HttpV2Routes.functionsJobs(master)

      Post(s"/v2/api/functions/x/jobs", JsMap("1" -> "Hello".js)) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should return history for function") {
      val execution = mock[ExecutionService]
      val master = mock[MainService]
      when(master.execution).thenReturn(execution)

      when(execution.getHistory(any[JobDetailsRequest])).thenSuccess(JobDetailsResponse(
        Seq(JobDetails("id", "1",
          JobParams("path", "className", JsMap.empty, Action.Execute),
          "context", None, JobDetails.Source.Http)
        ), 1
      ))

      val route = HttpV2Routes.functionsJobs(master)

      Get("/v2/api/functions/id/jobs?status=started") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val jobs = responseAs[Seq[JobDetails]]
        jobs.size shouldBe 1
      }
      Get("/v2/api/functions/id/jobs?status=started&paginate=true") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[JobDetailsResponse]
        rsp.jobs.size shouldBe 1
      }
    }


  }

  describe("function creation") {

    val functionData = FunctionInfoData(
      name="test",
      lang="scala",
      path= "path",
      className="Test",
      defaultContext = "foo"
    )

    it("should update function on create if function created") {

      val functions = mock[FunctionsService]

      val test = FunctionConfig("test", "test", "test", "default")

      when(functions.hasFunction(any[String])).thenSuccess(false)
      when(functions.update(any[FunctionConfig]))
        .thenSuccess(FunctionInfoData(
          lang = "python",
          path = "test",
          defaultContext = "foo",
          className = "test",
          name = "test"
        ))

      val route = HttpV2Routes.functionsCrud(functions)

      Post("/v2/api/functions", test.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should return different entity when forcibly update") {
      val functions = mock[FunctionsService]

      val test = FunctionConfig("test", "test", "test", "default")
      when(functions.updateConfig(any[FunctionConfig])).thenSuccess(test)

      val route = HttpV2Routes.functionsCrud(functions)

      Post("/v2/api/functions?force=true", test.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[FunctionConfig] shouldBe test
      }
    }

    it("should update function if it exists") {
      val functions = mock[FunctionsService]

      val test = FunctionConfig("test", "test", "test", "default")
      when(functions.hasFunction(any[String])).thenSuccess(true)
      when(functions.update(any[FunctionConfig]))
        .thenSuccess(FunctionInfoData(
          lang = "python",
          path = "test",
          defaultContext = "default",
          className = "test",
          name = "test"
        ))


      val route = HttpV2Routes.functionsCrud(functions)

      Put("/v2/api/functions", test.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[FunctionConfig] shouldBe test
      }
    }

    it("should not update function if it do not exist") {
      val functions = mock[FunctionsService]

      val test = FunctionConfig("test", "test", "test", "default")
      when(functions.hasFunction(any[String])).thenSuccess(false)

      val route = HttpV2Routes.functionsCrud(functions)

      Put("/v2/api/functions", test.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    it("should fail with invalid data for function") {
      val functions = mock[FunctionsService]

      val functionConfig = FunctionConfig("name", "path", "className", "context")
      when(functions.hasFunction(any[String])).thenReturn(Future.successful(false))
      when(functions.update(any[FunctionConfig])).thenReturn(Future.failed(new Exception("test failure")))

      val route = HttpV2Routes.functionsCrud(functions)

      Post("/v2/api/functions", functionConfig.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }

    }

    it("should delete function") {
      val functions = mock[FunctionsService]
      when(functions.delete(any[String])).thenSuccess(Some(FunctionInfoData(
        name="test",
        lang="scala",
        path= "path",
        className="Test",
        defaultContext = "foo"
      )))
      val route = HttpV2Routes.functionsCrud(functions)

      Delete(s"/v2/api/functions/x") ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }
  }


  describe("jobs") {

    val jobDetails = JobDetails(
      params = JobParams("path", "className", JsMap.empty, Action.Execute),
      jobId = "id",
      source = Source.Http,
      function = "function",
      context = "context",
      externalId = None
    )

    it("should return jobs") {
      val execution = mock[ExecutionService]
      val master = mock[MainService]
      when(master.execution).thenReturn(execution)
      when(execution.getHistory(any[JobDetailsRequest]))
        .thenSuccess(JobDetailsResponse(Seq(jobDetails), 1))

      val route = HttpV2Routes.jobsRoutes(master)

      Get(s"/v2/api/jobs") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[Seq[JobDetails]]
      }
      Get(s"/v2/api/jobs?paginate=true") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[JobDetailsResponse]
      }
    }

    it("should return jobs status by id") {
      val execution = mock[ExecutionService]
      val master = mock[MainService]
      when(master.execution).thenReturn(execution)
      when(execution.jobStatusById(any[String]))
        .thenSuccess(Some(jobDetails))

      val route = HttpV2Routes.jobsRoutes(master)

      Get(s"/v2/api/jobs/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }
    it("should return 400 on logs request when job not found") {
      val execution = mock[ExecutionService]
      val master = mock[MainService]
      when(master.execution).thenReturn(execution)
      when(execution.jobStatusById(any[String]))
        .thenSuccess(None)

      val route = HttpV2Routes.jobsRoutes(master)
      Get(s"/v2/api/jobs/id/logs") ~> route ~> check {
        status shouldBe StatusCodes.NotFound
      }
    }
    it("should return 200 empty response on logs request when job log file not exists") {
      val execution = mock[ExecutionService]
      val master = mock[MainService]
      val logStorageMappings = mock[LogStoragePaths]
      when(master.execution).thenReturn(execution)
      when(master.logsPaths).thenReturn(logStorageMappings)
      when(execution.jobStatusById(any[String]))
        .thenSuccess(Some(jobDetails))
      when(logStorageMappings.pathFor(any[String]))
        .thenReturn(Paths.get(".", UUID.randomUUID().toString))
      val route = HttpV2Routes.jobsRoutes(master)
      Get(s"/v2/api/jobs/id/logs") ~> route ~> check {
        status shouldBe StatusCodes.OK
        responseAs[String] shouldBe ""
      }

    }
    it("should cancel job") {
      val execution = mock[ExecutionService]
      val master = mock[MainService]
      when(master.execution).thenReturn(execution)
      when(execution.stopJob(any[String])).thenSuccess(Some(jobDetails))

      val route = HttpV2Routes.jobsRoutes(master)
      Delete(s"/v2/api/jobs/id") ~> route ~> check {
        status shouldBe StatusCodes.OK
        val rsp = responseAs[Option[JobDetails]]
        rsp.isDefined shouldBe true
      }
    }

  }

  describe("contexts") {

    class TestCrud extends ContextsCRUDLike {
      def create(req: ContextCreateRequest): Future[ContextConfig] = ???
      def getAll(): Future[Seq[ContextConfig]] = ???
      def update(a: ContextConfig): Future[ContextConfig] = ???
      def get(id: String): Future[Option[ContextConfig]] = ???
      def delete(id: String): Future[Option[ContextConfig]] = ???
    }

    it("should create context") {
      val crud = new TestCrud {
        override def create(req: ContextCreateRequest): Future[ContextConfig] = Future.successful(FooContext)
      }

      val route = HttpV2Routes.contextsRoutes(crud)
      val req = ContextCreateRequest("yoyo", workerMode = Some(RunMode.ExclusiveContext))
      Post(s"/v2/api/contexts", req.toEntity) ~> route ~> check {
        status shouldBe StatusCodes.OK
      }
    }

    it("should delete context") {
      val crud = new TestCrud {
        override def delete(id: String): Future[Option[ContextConfig]] = Future.successful(Some(FooContext))
      }

      val route = HttpV2Routes.contextsRoutes(crud)
      Delete(s"/v2/api/contexts/foo") ~> route ~> check {
        status shouldBe StatusCodes.OK
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

      when(master.runJob(any[FunctionStartRequest], any[Source]))
        .thenFailure(new IllegalArgumentException("argument missing"))

      val route = HttpV2Routes.apiRoutes(master, mock[ArtifactRepository], "")
      Post(s"/v2/api/functions/x/jobs", JsMap("1" -> "Hello".js)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
      }
    }

    it("should return 500 on future`s any exception except iae") {
      val master = mock[MainService]

      when(master.runJob(any[FunctionStartRequest], any[Source]))
        .thenFailure(new RuntimeException("some exception"))

      val route = HttpV2Routes.apiRoutes(master, mock[ArtifactRepository], "")

      Post(s"/v2/api/functions/x/jobs", JsMap("1" -> "Hello".js)) ~> route ~> check {
        status shouldBe StatusCodes.InternalServerError
      }
    }
  }

}
