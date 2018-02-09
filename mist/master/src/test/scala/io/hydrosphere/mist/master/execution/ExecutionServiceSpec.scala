package io.hydrosphere.mist.master.execution

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.jvmjob.FunctionInfoData
import io.hydrosphere.mist.master.Messages.JobExecution._
import io.hydrosphere.mist.master.execution.workers.WorkerHub
import io.hydrosphere.mist.master.models.{JobStartRequest, RunMode}
import io.hydrosphere.mist.master.store.JobRepository
import io.hydrosphere.mist.master.{JobDetails, TestData, TestUtils}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExecutionServiceSpec extends TestKit(ActorSystem("testMasterService"))
  with FunSpecLike
  with Matchers
  with MockitoSugar
  with TestData {

  describe("jobs starting") {

    it("should start job") {
      val execution = TestProbe()
      val repo = mock[JobRepository]
      val hub = mock[WorkerHub]
      when(repo.update(any[JobDetails])).thenSuccess(())

      val service = new ExecutionService(execution.ref, hub, repo)

      val future = service.startJob(
        JobStartRequest(
          id = "id",
          function = FunctionInfoData("name", path="path", className="className", defaultContext="context"),
          context = TestUtils.contextSettings.default,
          parameters = Map("1" -> 2),
          source = JobDetails.Source.Http,
          externalId = None
      ))

      execution.expectMsgType[RunJobCommand]
      execution.reply(ExecutionInfo(req = mkRunReq("id")))

      val executionInfo = Await.result(future, Duration.Inf)
      executionInfo.request.id shouldBe "id"
    }

  }

  describe("jobs stopping") {

    it("should stop job") {
      //TODO
      val execution = TestProbe()
      val repo = mock[JobRepository]
      val hub = mock[WorkerHub]
      when(repo.get(any[String])).thenSuccess(Some(mkDetails(JobDetails.Status.Started)))

      val service = new ExecutionService(execution.ref, hub, repo)

      val future = service.stopJob("id")

      execution.expectMsgType[CancelJobCommand]
      execution.reply(())

      when(repo.get(any[String])).thenSuccess(Some(mkDetails(JobDetails.Status.Canceled)))

      val details = Await.result(future, Duration.Inf)
      details.get.status shouldBe JobDetails.Status.Canceled
    }

  }

}
