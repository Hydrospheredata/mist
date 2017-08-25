package io.hydrosphere.mist.master

import java.io.File

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.master.artifact.{ArtifactRepository, EndpointArtifactKeyProvider, ArtifactKeyProvider}
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models._
import org.mockito.Mockito.{doReturn, spy}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class MasterServiceSpec extends TestKit(ActorSystem("testMasterService"))
  with FunSpecLike
  with Matchers
  with MockitoSugar {

  import TestUtils._

  it("should run job") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStorageMappings]
    val artifactRepo = mock[ArtifactRepository]
    val artifactKeyProvider = mock[ArtifactKeyProvider[EndpointConfig, String]]

    when(endpoints.get(any[String]))
      .thenSuccess(Some(EndpointConfig("name", "path.py", "MyJob", "namespace")))

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(TestUtils.contextSettings.default)

    when(artifactRepo.get(any[String]))
      .thenReturn(Some(new File("path.py")))

    when(jobService.startJob(any[JobStartRequest])).thenSuccess(ExecutionInfo(
        req = RunJobRequest("id", JobParams("path.py", "MyJob", Map("x" -> 1), Action.Execute)),
        status = JobDetails.Status.Queued
      ))

    val service = new MasterService(jobService, endpoints, contexts, logs, artifactRepo, artifactKeyProvider)

    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    val runInfo = service.runJob(req, Source.Http).await
    runInfo shouldBe defined
  }

  it("should return failed future on validating params") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStorageMappings]
    val artifactRepo = mock[ArtifactRepository]
    val jvmMock = mock[JvmJobInfo]
    val artifactKeyProvider = mock[ArtifactKeyProvider[EndpointConfig, String]]

    val info = FullEndpointInfo(
      EndpointConfig("name", "path", "MyJob", "namespace"),
      jvmMock
    )
    when(jvmMock.validateAction(any[Map[String, Any]], any[Action]))
      .thenReturn(Left(new IllegalArgumentException("INVALID")))

    val service = new MasterService(jobService, endpoints, contexts, logs, artifactRepo, artifactKeyProvider)

    val spiedMasterService = spy(service)

    doReturn(Future.successful(Some(info)))
      .when(spiedMasterService)
      .endpointInfo(any[String])

    val req = EndpointStartRequest("scalajob", Map("notNumbers" -> Seq(1, 2, 3)), Some("externalId"))
    val f = spiedMasterService.runJob(req, Source.Http)

    ScalaFutures.whenReady(f.failed) { ex =>
      ex shouldBe a[IllegalArgumentException]
    }

  }
  it("should fail job execution when context config filled with incorrect worker mode") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStorageMappings]
    val artifactRepository = mock[ArtifactRepository]
    val service = new MasterService(jobService, endpoints, contexts, logs, artifactRepository)
    val spiedService = spy(service)
    val fullInfo = FullEndpointInfo(
      EndpointConfig("name", "path", "MyJob", "namespace"),
      PyJobInfo
    )
    doReturn(Future.successful(Some(fullInfo)))
      .when(spiedService)
      .endpointInfo(any[String])

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
    val runInfo = spiedService.runJob(req, Source.Http)

    ScalaFutures.whenReady(runInfo.failed) {ex =>
      ex shouldBe an[IllegalArgumentException]
    }

  }

}
