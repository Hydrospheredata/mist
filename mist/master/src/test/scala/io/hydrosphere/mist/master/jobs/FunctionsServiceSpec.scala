package io.hydrosphere.mist.master.jobs

import java.io.File
import java.nio.file.Paths

import akka.actor.{ActorSystem, Status}
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData.{Action, GetAllFunctions, GetFunctionInfo, ValidateFunctionParameters}
import io.hydrosphere.mist.core.{ExtractedFunctionData, FunctionInfoData, MockitoSugar}
import io.hydrosphere.mist.core.ExtractedFunctionData
import io.hydrosphere.mist.master.TestData
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{Contexts, ContextsStorage, FunctionConfigStorage}
import io.hydrosphere.mist.master.models.{ContextConfig, FunctionConfig}
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}
import mist.api.data._
import mist.api.encoding.JsSyntax._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits._

class FunctionsServiceSpec extends TestKit(ActorSystem("test"))
  with FunSpecLike
  with MockitoSugar
  with Matchers
  with BeforeAndAfterAll {


  val baseDir = "./target/test-jobs"
  val jobPath = Paths.get(baseDir, "testJob.jar").toString

  val testContexts = TestContexts.mk(TestData.FooContext.copy(name = "default"), TestData.FooContext)

  override def beforeAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
    FileUtils.forceMkdir(f)
    FileUtils.touch(new File(jobPath))
  }

  override def afterAll(): Unit = {
    val f = new File(baseDir)
    FileUtils.deleteQuietly(f)
  }

  describe("by function config methods") {
    it("should get job info from function") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfoByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ))
      probe.expectMsgType[GetFunctionInfo]
      probe.reply(ExtractedFunctionData(
        name="test",
        lang="scala"
      ))

      val result = Await.result(f, Duration.Inf)

      result shouldBe FunctionInfoData(
        name="test",
        lang="scala",
        path=jobPath,
        className="Test",
        defaultContext = "foo"
      )
    }

    it("should validate job parameters by function") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.validateFunctionParamsByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ), JsMap.empty)

      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Success(()))

      Await.result(f, Duration.Inf)
    }

    it("should return none on get job info when artifact not found") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfoByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }

    it("should fail on validate job parameters when artifact not found") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)

      val f = functionInfoService.validateFunctionParamsByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ), JsMap.empty)

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }

    }

    it("should fail when get job info from actor failed") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfoByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ))
      probe.expectMsgType[GetFunctionInfo]
      probe.reply(Status.Failure(new IllegalArgumentException("invalid")))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }
    it("should fail validate job if actor failed") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.validateFunctionParamsByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ), JsMap.empty)

      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Failure(new IllegalArgumentException("invalid")))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }
  }

  describe("by function id methods") {
    it("should get job info from function id") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfo("test")

      probe.expectMsgType[GetFunctionInfo]
      probe.reply(ExtractedFunctionData(
        name="test",
        lang="scala"
      ))

      val result = Await.result(f, Duration.Inf)

      result shouldBe defined
      result.get shouldBe FunctionInfoData(
        name="test",
        lang="scala",
        path=jobPath,
        className="Test",
        defaultContext = "foo"
      )
    }

    it ("should validate job parameters by function id") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.validateFunctionParams("test", JsMap.empty)

      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Success(()))

      val result = Await.result(f, Duration.Inf)

      result shouldBe defined

    }

    it("should return none on get job info when function not found") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(None)

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfo("test")

      val result = Await.result(f, Duration.Inf)

      result should not be defined

    }

    it("should return none on get job info when artifact not found") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfo("test")

      val result = Await.result(f, Duration.Inf)

      result should not be defined

    }

    it("should return none on validate job parameters when function not found") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(None)

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.validateFunctionParams("test", JsMap.empty)

      val result = Await.result(f, Duration.Inf)

      result should not be defined
    }

    it("should return none on validate job parameters when artifact not found") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.validateFunctionParams("test", JsMap.empty)

      val result = Await.result(f, Duration.Inf)

      result should not be defined
    }

    it("should fail get job info from actor failed") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.getFunctionInfo("test")
      probe.expectMsgType[GetFunctionInfo]
      probe.reply(Status.Failure(new IllegalArgumentException("invalid")))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }
    it("should fail validate job if actor failed") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.validateFunctionParams("test", JsMap.empty)
      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Failure(new IllegalArgumentException("invalid")))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }
  }

  describe("allInfos") {

    it("should return all job infos") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.all)
        .thenSuccess(Seq(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.allFunctions
      probe.expectMsgType[GetAllFunctions]
      probe.reply(Seq(ExtractedFunctionData(name="test")))

      val response = Await.result(f, Duration.Inf)
      response.size shouldBe 1
    }

    it("shouldn't fail with empty functions") {
      val probe = TestProbe()
      val functions = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(functions.all).thenSuccess(Seq.empty)

      when(artifactRepo.get(any[String])).thenReturn(Some(new File(jobPath)))

      val functionInfoService = new FunctionsService(probe.ref, functions, testContexts, artifactRepo)
      val f = functionInfoService.allFunctions

      val response = Await.result(f, Duration.Inf)
      response.size shouldBe 0
    }

  }

  class TestContexts(
    default: ContextConfig,
    ctxs: Map[String, ContextConfig]
  ) extends Contexts {
    override def get(name: String): Future[Option[ContextConfig]] = Future.successful(ctxs.get(name))
    override def getOrDefault(name: String): Future[ContextConfig] = get(name).map(_.getOrElse(default))
    override def all: Future[Seq[ContextConfig]] = Future.successful(ctxs.values.toSeq)
    override def defaultConfig: ContextConfig = default
  }

  object TestContexts {

    def mk(default: ContextConfig, ctxs: ContextConfig*): Contexts = {
      new TestContexts(default, ctxs.map(c => c.name -> c).toMap)
    }
  }
}
