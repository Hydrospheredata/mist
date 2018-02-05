package io.hydrosphere.mist.master.jobs

import java.io.File
import java.nio.file.Paths

import akka.actor.{ActorSystem, Status}
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData.{Action, GetAllFunctions, GetFunctionInfo, ValidateFunctionParameters}
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.{ExtractedData, FunctionInfoData}
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.FunctionConfigStorage
import io.hydrosphere.mist.master.models.FunctionConfig
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits._

class FunctionInfoServiceSpec extends TestKit(ActorSystem("test"))
  with FunSpecLike
  with MockitoSugar
  with Matchers
  with BeforeAndAfterAll {


  val baseDir = "./target/test-jobs"
  val jobPath = Paths.get(baseDir, "testJob.jar").toString

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

  describe("by endpoint config methods") {
    it("should get job info from endpoint") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfoByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ))
      probe.expectMsgType[GetFunctionInfo]
      probe.reply(ExtractedData(
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

    it("should validate job parameters by endpoint") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.validateFunctionParamsByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ), Map.empty)

      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Success(()))

      Await.result(f, Duration.Inf)
    }

    it("should return none on get job info when artifact not found") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfoByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }

    it("should fail on validate job parameters when artifact not found") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)

      val f = jobInfoProviderService.validateFunctionParamsByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ), Map.empty)

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }

    }

    it("should fail when get job info from actor failed") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfoByConfig(FunctionConfig(
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
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.validateFunctionParamsByConfig(FunctionConfig(
        "test", jobPath, "Test", "foo"
      ), Map.empty)

      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Failure(new IllegalArgumentException("invalid")))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }
  }

  describe("by endpoint id methods") {
    it("should get job info from endpoint id") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfo("test")

      probe.expectMsgType[GetFunctionInfo]
      probe.reply(ExtractedData(
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

    it ("should validate job parameters by endpoint id") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.validateFunctionParams("test", Map.empty)

      probe.expectMsgType[ValidateFunctionParameters]
      probe.reply(Status.Success(()))

      val result = Await.result(f, Duration.Inf)

      result shouldBe defined

    }

    it("should return none on get job info when endpoint not found") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(None)

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfo("test")

      val result = Await.result(f, Duration.Inf)

      result should not be defined

    }

    it("should return none on get job info when artifact not found") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfo("test")

      val result = Await.result(f, Duration.Inf)

      result should not be defined

    }

    it("should return none on validate job parameters when endpoint not found") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(None)

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.validateFunctionParams("test", Map.empty)

      val result = Await.result(f, Duration.Inf)

      result should not be defined
    }

    it("should return none on validate job parameters when artifact not found") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(None)

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.validateFunctionParams("test", Map.empty)

      val result = Await.result(f, Duration.Inf)

      result should not be defined
    }

    it("should fail get job info from actor failed") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.getFunctionInfo("test")
      probe.expectMsgType[GetFunctionInfo]
      probe.reply(Status.Failure(new IllegalArgumentException("invalid")))

      intercept[IllegalArgumentException] {
        Await.result(f, Duration.Inf)
      }
    }
    it("should fail validate job if actor failed") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.get(any[String]))
        .thenSuccess(Some(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.validateFunctionParams("test", Map.empty)
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
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.all)
        .thenSuccess(Seq(FunctionConfig(
          "test", jobPath, "Test", "foo"
        )))

      when(artifactRepo.get(any[String]))
        .thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.allFunctions
      probe.expectMsgType[GetAllFunctions]
      probe.reply(Seq(ExtractedData(name="test")))

      val response = Await.result(f, Duration.Inf)
      response.size shouldBe 1
    }

    it("shouldn't fail with empty endpoints") {
      val probe = TestProbe()
      val endpoints = mock[FunctionConfigStorage]
      val artifactRepo = mock[ArtifactRepository]

      when(endpoints.all).thenSuccess(Seq.empty)

      when(artifactRepo.get(any[String])).thenReturn(Some(new File(jobPath)))

      val jobInfoProviderService = new FunctionInfoService(probe.ref, endpoints, artifactRepo)
      val f = jobInfoProviderService.allFunctions

      val response = Await.result(f, Duration.Inf)
      response.size shouldBe 0
    }

  }
}
