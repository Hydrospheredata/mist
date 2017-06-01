package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.Messages.StatusMessages.Register
import io.hydrosphere.mist.Messages.WorkerMessages.RunJobCommand
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.models.JobStartRequest
import org.mockito.Matchers._
import org.mockito.Mockito._
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

    when(endpoints.getDefinition(any[String]))
      .thenReturn(Some(JobDefinition("name", "path.py", "MyJob", "namespace")))

    val service = new MasterService(workerManager.ref, statusService.ref, endpoints)

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

}
