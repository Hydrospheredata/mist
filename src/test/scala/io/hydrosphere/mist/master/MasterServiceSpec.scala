package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.Messages.StatusMessages.Register
import io.hydrosphere.mist.Messages.WorkerMessages.RunJobCommand
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.jar.JobsLoader
import io.hydrosphere.mist.master.models.JobStartRequest
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class MasterServiceSpec extends TestKit(ActorSystem("testMasterService"))
  with FunSpecLike
  with Matchers {

  it("should run job") {
    val workerManager = TestProbe()
    val statusService = TestProbe()

    val endpoints = mock(classOf[JobEndpoints])
    val contextSettings = mock(classOf[ContextsSettings])

    when(endpoints.getInfo(any[String]))
      .thenReturn(Some(PyJobInfo(JobDefinition("name", "path.py", "MyJob", "namespace"))))

    val service = new MasterService(workerManager.ref, statusService.ref, endpoints, contextSettings)

    val req = JobStartRequest("name", Map("x" -> 1), Some("externalId"))
    val f = service.runJob(req, Source.Http)

    val executionInfo = ExecutionInfo(
      req = RunJobRequest("id", JobParams("path.py", "MyJob", Map("x" -> 1), Action.Execute)),
      status = JobDetails.Status.Queued
    )

    statusService.expectMsgClass(classOf[Register])
    statusService.reply(akka.actor.Status.Success(()))
    workerManager.expectMsgClass(classOf[RunJobCommand])
    workerManager.reply(executionInfo)

    Await.result(f, 10 seconds)
  }

  it("should return failed future on validating params") {
    val workerManager = TestProbe()
    val statusService = TestProbe()

    val endpoints = mock(classOf[JobEndpoints])
    val contextSettings = mock(classOf[ContextsSettings])

    val testJobClass = io.hydrosphere.mist.jobs.jar.MultiplyJob
    val jvmInfo = JvmJobInfo(
      JobDefinition("scalajob", "path_to_jar.jar", "ScalaJob", "namespace"),
      JobsLoader.Common.loadJobClass(testJobClass.getClass.getCanonicalName).get
    )

    when(endpoints.getInfo(any[String]))
      .thenReturn(Some(jvmInfo))

    val service = new MasterService(workerManager.ref, statusService.ref, endpoints, contextSettings)

    val req = JobStartRequest("scalajob", Map("notNumbers" -> Seq(1, 2, 3)), Some("externalId"))
    val f = service.runJob(req, Source.Http)

    ScalaFutures.whenReady(f.failed) { ex =>
      ex shouldBe a[IllegalArgumentException]
    }

  }

}
