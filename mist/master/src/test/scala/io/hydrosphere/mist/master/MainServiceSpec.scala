package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.JobInfoData
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.execution.ExecutionInfo
import io.hydrosphere.mist.master.jobs.JobInfoProviderService
import io.hydrosphere.mist.master.models.RunMode.{ExclusiveContext, Shared}
import io.hydrosphere.mist.master.models._
import mist.api.args.ArgInfo
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify}
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class MainServiceSpec extends TestKit(ActorSystem("testMasterService"))
  with FunSpecLike
  with Matchers
  with MockitoSugar {

  import TestUtils._

  it("should run job") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStoragePaths]
    val jobInfoProviderService = mock[JobInfoProviderService]

    val artifactRepo = mock[ArtifactRepository]

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(TestUtils.contextSettings.default)

    when(jobInfoProviderService.getJobInfo(any[String]))
      .thenSuccess(Some(JobInfoData(
        name = "name",
        path = "path.py",
        className = "MyJob",
        defaultContext = "namespace"
      )))

    when(jobInfoProviderService.validateJob(any[String], any[Map[String, Any]]))
      .thenSuccess(Some(()))

    when(jobService.startJob(any[JobStartRequest])).thenSuccess(ExecutionInfo(
      req = RunJobRequest("id", JobParams("path.py", "MyJob", Map("x" -> 1), Action.Execute))
    ))

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProviderService)

    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    val runInfo = service.runJob(req, JobDetails.Source.Http).await
    runInfo shouldBe defined
  }

  it("should return failed future on validating params") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStoragePaths]
    val artifactRepo = mock[ArtifactRepository]
    val jobInfoProvider = mock[JobInfoProviderService]

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider)

    when(jobInfoProvider.validateJob(any[String], any[Map[String, Any]]))
      .thenFailure(new IllegalArgumentException("INVALID"))
    when(jobInfoProvider.getJobInfo(any[String]))
      .thenSuccess(Some(JobInfoData(
        "test",
        "test",
        "Test",
        "foo",
        JobInfoData.PythonLang
      )))

    val req = EndpointStartRequest("scalajob", Map("notNumbers" -> Seq(1, 2, 3)), Some("externalId"))

    val f = service.runJob(req, JobDetails.Source.Http)

    intercept[IllegalArgumentException] {
      Await.result(f, 30 seconds)
    }

  }
  it("should fail job execution when context config filled with incorrect worker mode") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStoragePaths]
    val artifactRepository = mock[ArtifactRepository]
    val jobInfoProvider = mock[JobInfoProviderService]

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider)

    when(jobInfoProvider.validateJob(any[String], any[Map[String, Any]]))
      .thenSuccess(Some(()))

    when(jobInfoProvider.getJobInfo(any[String]))
      .thenSuccess(Some(JobInfoData(
        "test",
        "test",
        "Test",
        "foo",
        JobInfoData.PythonLang
      )))

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(ContextConfig(
        "default",
        Map.empty,
        Duration.Inf,
        20,
        precreated = false,
        "",
        "wrong_mode",
        1 seconds
      ))


    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    val runInfo = service.runJob(req, JobDetails.Source.Http)
    intercept[IllegalArgumentException] {
      Await.result(runInfo, 30 seconds)
    }

  }

  it("should select exclusive run mode on streaming jobs") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStoragePaths]
    val artifactRepository = mock[ArtifactRepository]
    val jobInfoProvider = mock[JobInfoProviderService]

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider)

    when(jobInfoProvider.validateJob(any[String], any[Map[String, Any]]))
      .thenSuccess(Some(()))

    when(jobInfoProvider.getJobInfo(any[String]))
      .thenSuccess(Some(JobInfoData(
        "test",
        "test",
        "Test",
        "foo",
        lang = JobInfoData.PythonLang,
        tags = Seq(ArgInfo.StreamingContextTag)
      )))

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(ContextConfig(
        "default",
        Map.empty,
        Duration.Inf,
        20,
        precreated = false,
        "",
        "shared",
        1 seconds
      ))
    when(jobService.startJob(any[JobStartRequest]))
      .thenSuccess(ExecutionInfo(RunJobRequest("test", JobParams("test", "test", Map.empty, Action.Execute))))

    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    Await.result(service.runJob(req, JobDetails.Source.Http), 30 seconds)
    val argCapture = ArgumentCaptor.forClass(classOf[JobStartRequest])
    verify(jobService, times(1)).startJob(argCapture.capture())
    argCapture.getValue.runMode shouldBe ExclusiveContext(None)
  }


  it("should select run mode from context config when job is not streaming") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStoragePaths]
    val artifactRepository = mock[ArtifactRepository]
    val jobInfoProvider = mock[JobInfoProviderService]

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider)

    when(jobInfoProvider.validateJob(any[String], any[Map[String, Any]]))
      .thenSuccess(Some(()))

    when(jobInfoProvider.getJobInfo(any[String]))
      .thenSuccess(Some(JobInfoData(
        "test",
        "test",
        "Test",
        "foo",
        JobInfoData.PythonLang,
        tags = Seq(ArgInfo.SqlContextTag)
      )))

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(ContextConfig(
        "default",
        Map.empty,
        Duration.Inf,
        20,
        precreated = false,
        "",
        "shared",
        1 seconds
      ))
    when(jobService.startJob(any[JobStartRequest]))
      .thenSuccess(ExecutionInfo(RunJobRequest("test", JobParams("test", "test", Map.empty, Action.Execute))))

    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    Await.result(service.runJob(req, JobDetails.Source.Http), 30 seconds)
    val argCapture = ArgumentCaptor.forClass(classOf[JobStartRequest])
    verify(jobService, times(1)).startJob(argCapture.capture())
    argCapture.getValue.runMode shouldBe Shared
  }
}
