package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.MockitoSugar
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.data.{ContextsStorage, EndpointsStorage}
import io.hydrosphere.mist.master.logging.LogStorageMappings
import io.hydrosphere.mist.master.models._
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

    val fullInfo = FullEndpointInfo(
      EndpointConfig("name", "path", "MyJob", "namespace"),
      PyJobInfo
    )

    when(endpoints.getFullInfo(any[String]))
      .thenSuccess(Some(fullInfo))

    when(contexts.getOrDefault(any[String]))
      .thenSuccess(TestUtils.contextSettings.default)

    when(jobService.startJob(any[JobStartRequest])).thenSuccess(
      ExecutionInfo(
        req = RunJobRequest("id", JobParams("path.py", "MyJob", Map("x" -> 1), Action.Execute)),
        status = JobDetails.Status.Queued
      )
    )

    val service = new MasterService(jobService, endpoints, contexts, logs)

    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    val runInfo = service.runJob(req, Source.Http).await
    runInfo.isDefined shouldBe true
  }

  it("should return failed future on validating params") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStorageMappings]

    val jvmMock = mock[JvmJobInfo]
    val info = FullEndpointInfo(
      EndpointConfig("name", "path", "MyJob", "namespace"),
      jvmMock
    )
    when(jvmMock.validateAction(any[Map[String, Any]], any[Action]))
      .thenReturn(Left(new IllegalArgumentException("INVALID")))

    when(endpoints.getFullInfo(any[String]))
      .thenSuccess(Some(info))

    val service = new MasterService(jobService, endpoints, contexts, logs)

    val req = EndpointStartRequest("scalajob", Map("notNumbers" -> Seq(1, 2, 3)), Some("externalId"))
    val f = service.runJob(req, Source.Http)

    ScalaFutures.whenReady(f.failed) { ex =>
      ex shouldBe a[IllegalArgumentException]
    }

  }
  it("should fail job execution when context config filled with incorrect worker mode") {
    val endpoints = mock[EndpointsStorage]
    val contexts = mock[ContextsStorage]
    val jobService = mock[JobService]
    val logs = mock[LogStorageMappings]

    val fullInfo = FullEndpointInfo(
      EndpointConfig("name", "path", "MyJob", "namespace"),
      PyJobInfo
    )

    when(endpoints.getFullInfo(any[String]))
      .thenSuccess(Some(fullInfo))

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

    val service = new MasterService(jobService, endpoints, contexts, logs)

    val req = EndpointStartRequest("name", Map("x" -> 1), Some("externalId"))
    val runInfo = service.runJob(req, Source.Http)

    ScalaFutures.whenReady(runInfo.failed) {ex =>
      ex shouldBe an[IllegalArgumentException]
    }

  }

}
