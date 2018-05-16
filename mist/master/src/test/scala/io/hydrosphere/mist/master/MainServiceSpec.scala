package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}
import io.hydrosphere.mist.core.{FunctionInfoData, MockitoSugar}
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, FunctionConfigStorage}
import io.hydrosphere.mist.master.execution.{ExecutionInfo, ExecutionService}
import io.hydrosphere.mist.master.jobs.FunctionInfoService
import io.hydrosphere.mist.master.models.RunMode.{ExclusiveContext, Shared}
import io.hydrosphere.mist.master.models._
import mist.api.data.{JsList, JsMap}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.scalatest.{FunSpecLike, Matchers}
import mist.api.data.JsSyntax._

import scala.concurrent.Await
import scala.concurrent.duration._

class MainServiceSpec extends TestKit(ActorSystem("testMasterService"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with TestUtils
  with TestData {

  it("should run job") {
    val functions = mock[FunctionConfigStorage]
    val contexts = mock[ContextsStorage]
    val execution = mock[ExecutionService]
    val logs = mock[LogStoragePaths]
    val jobInfoProviderService = mock[FunctionInfoService]

    val artifactRepo = mock[ArtifactRepository]

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(TestUtils.contextSettings.default)

    when(jobInfoProviderService.getFunctionInfo(any[String]))
      .thenSuccess(Some(FunctionInfoData(
        name = "name",
        path = "path.py",
        className = "MyJob",
        defaultContext = "namespace"
      )))

    when(jobInfoProviderService.validateFunctionParams(any[String], any[JsMap]))
      .thenSuccess(Some(()))

    when(execution.startJob(any[JobStartRequest])).thenSuccess(ExecutionInfo(
      req = RunJobRequest("id", JobParams("path.py", "MyJob", JsMap("x" -> 1.js), Action.Execute))
    ))

    val service = new MainService(execution, functions, contexts, logs, jobInfoProviderService)

    val req = FunctionStartRequest("name", JsMap("x" -> 1.js), Some("externalId"))
    val runInfo = service.runJob(req, JobDetails.Source.Http).await
    runInfo shouldBe defined
  }

  it("should return failed future on validating params") {
    val functions = mock[FunctionConfigStorage]
    val contexts = mock[ContextsStorage]
    val execution = mock[ExecutionService]
    val logs = mock[LogStoragePaths]
    val artifactRepo = mock[ArtifactRepository]
    val jobInfoProvider = mock[FunctionInfoService]

    val service = new MainService(execution, functions, contexts, logs, jobInfoProvider)

    when(jobInfoProvider.validateFunctionParams(any[String], any[JsMap]))
      .thenFailure(new IllegalArgumentException("INVALID"))
    when(jobInfoProvider.getFunctionInfo(any[String]))
      .thenSuccess(Some(FunctionInfoData(
        "test",
        "test",
        "Test",
        "foo",
        FunctionInfoData.PythonLang
      )))

    val req = FunctionStartRequest("scalajob", JsMap("notNumbers" -> JsList(Seq(1.js, 2.js, 3.js))), Some("externalId"))

    val f = service.runJob(req, JobDetails.Source.Http)

    intercept[IllegalArgumentException] {
      Await.result(f, 30 seconds)
    }

  }

  it("should use context in request") {
    val functions = mock[FunctionConfigStorage]
    val contexts = mock[ContextsStorage]
    val execution = mock[ExecutionService]
    val logs = mock[LogStoragePaths]
    val artifactRepository = mock[ArtifactRepository]
    val jobInfoProvider = mock[FunctionInfoService]

    val service = new MainService(execution, functions, contexts, logs, jobInfoProvider)

    when(jobInfoProvider.validateFunctionParams(any[String], any[JsMap]))
      .thenSuccess(Some(()))

    when(jobInfoProvider.getFunctionInfo(any[String]))
      .thenSuccess(Some(functionInfoData))

    when(contexts.getOrDefault(any[String])).thenSuccess(FooContext)
    when(execution.startJob(any[JobStartRequest]))
      .thenSuccess(ExecutionInfo(RunJobRequest("test", JobParams("test", "test", JsMap.empty, Action.Execute))))

    val req = FunctionStartRequest("name", JsMap("x" -> 1.js), Some("externalId"))
    Await.result(service.runJob(req, JobDetails.Source.Http), 30 seconds)

    val argCapture = ArgumentCaptor.forClass(classOf[JobStartRequest])
    verify(execution, times(1)).startJob(argCapture.capture())

    val jobStartReq = argCapture.getValue

    jobStartReq.function shouldBe functionInfoData
    jobStartReq.context shouldBe FooContext
  }
}
