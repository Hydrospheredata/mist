package io.hydrosphere.mist.master

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.core.CommonData.{Action, JobParams, RunJobRequest}
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.FullJobInfo
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.jobs.JobInfoProviderService
import io.hydrosphere.mist.master.models._
import org.mockito.Matchers.{eq => mockitoEq}
import org.mockito.Mockito.spy
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
      .thenSuccess(Some(FullJobInfo(
        name = "name",
        path = "path.py",
        className = "MyJob",
        defaultContext = "namespace"
      )))

    when(jobInfoProviderService.validateJob(any[String], any[Map[String, Any]], any[Action]))
      .thenSuccess(Some(()))

    when(jobService.startJob(any[JobStartRequest])).thenSuccess(ExecutionInfo(
      req = RunJobRequest("id", JobParams("path.py", "MyJob", Map("x" -> 1), Action.Execute)),
      status = JobDetails.Status.Queued
    ))

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProviderService, artifactRepo)

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

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider, artifactRepo)

    when(jobInfoProvider.validateJob(any[String], any[Map[String, Any]], any[Action]))
      .thenFailure(new IllegalArgumentException("INVALID"))
    when(jobInfoProvider.getJobInfo(any[String]))
      .thenSuccess(Some(FullJobInfo(
        lang = FullJobInfo.PythonLang,
        name = "test"
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

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider, artifactRepository)

    when(jobInfoProvider.validateJob(any[String], any[Map[String, Any]], any[Action]))
      .thenSuccess(Some(()))

    when(jobInfoProvider.getJobInfo(any[String]))
      .thenSuccess(Some(FullJobInfo(
        lang = FullJobInfo.PythonLang,
        name = "test"
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

  it("should return only existing endpoint jobs") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStoragePaths]
    val artifactRepository = mock[ArtifactRepository]
    val jobInfoProvider = mock[JobInfoProviderService]

    val service = new MainService(jobService, endpoints, contexts, logs, jobInfoProvider, artifactRepository)
    val spiedService = spy(service)
    val epConf = EndpointConfig("name", "path", "MyJob", "namespace")
    val noMatterEpConf = EndpointConfig("no_matter", "testpath", "MyJob2", "namespace")
    val fullInfo = FullJobInfo(
      lang = "python",
      defaultContext = "foo",
      path = "path",
      name = "name",
      className = "MyJob"
    )
    when(jobInfoProvider.getJobInfo(mockitoEq(epConf)))
      .thenSuccess(fullInfo)

    when(jobInfoProvider.getJobInfo(mockitoEq(noMatterEpConf)))
      .thenFailure(new RuntimeException("failed"))

    when(endpoints.all)
      .thenSuccess(Seq(epConf, noMatterEpConf))

    val endpointsInfo = spiedService.endpointsInfo.await
    endpointsInfo.size shouldBe 1
    endpointsInfo should contain allElementsOf Seq(fullInfo)

  }

}
