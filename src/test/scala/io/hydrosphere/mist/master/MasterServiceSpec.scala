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
      .thenReturn(Future.successful(Some(EndpointConfig("name", "path.py", "MyJob", "namespace"))))

    when(contexts.getOrDefault(any[String]))
      .thenReturn(Future.successful(TestUtils.contextSettings.default))

    when(artifactRepo.get(any[String]))
      .thenReturn(Some(new File("path.py")))

    when(jobService.startJob(
      any[String],
      any[EndpointConfig],
      any[ContextConfig],
      any[Map[String, Any]],
      any[RunMode],
      any[JobDetails.Source],
      any[Option[String]],
      any[Action]
    )).thenReturn(Future.successful(
      ExecutionInfo(
        req = RunJobRequest("id", JobParams("path.py", "MyJob", Map("x" -> 1), Action.Execute)),
        status = JobDetails.Status.Queued
      )
    ))

    val service = new MasterService(jobService, endpoints, contexts, logs, artifactRepo, artifactKeyProvider)

    val req = JobStartRequest("name", Map("x" -> 1), Some("externalId"))
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
      .when(spiedMasterService).endpointInfo(any[String])

    val req = JobStartRequest("scalajob", Map("notNumbers" -> Seq(1, 2, 3)), Some("externalId"))
    val f = spiedMasterService.runJob(req, Source.Http)

    ScalaFutures.whenReady(f.failed) { ex =>
      ex shouldBe a[IllegalArgumentException]
    }

  }

}
