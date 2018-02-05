package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.jvmjob.JobInfoData
import io.hydrosphere.mist.master.Messages.JobExecution._
import io.hydrosphere.mist.master.Messages.StatusMessages.{GetById, Register}
import io.hydrosphere.mist.master.models.{EndpointConfig, JobStartRequest, RunMode}
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}

class JobServiceSpec extends TestKit(ActorSystem("testMasterService"))
  with FunSpecLike
  with Matchers {

  describe("jobs starting") {

    it("should start job") {
      val workerManager = TestProbe()
      val statusService = TestProbe()

      val service = new JobService(workerManager.ref, statusService.ref)

      val future = service.startJob(
        JobStartRequest(
          id = "id",
          function = JobInfoData("name", path="path", className="className", defaultContext="context"),
          context = TestUtils.contextSettings.default,
          parameters = Map("1" -> 2),
          runMode = RunMode.Shared,
          source = JobDetails.Source.Http,
          externalId = None
      ))

      statusService.expectMsgType[Register]
      statusService.reply(())

      workerManager.expectMsgType[RunJobCommand]
      workerManager.reply(ExecutionInfo(req = mkRunReq("id")))

      val executionInfo = Await.result(future, Duration.Inf)
      executionInfo.request.id shouldBe "id"
    }

  }

  describe("jobs stopping") {

    val startedDetails = JobDetails(
      function = "endpoint",
      jobId = "jobId",
      params = JobParams("path", "class", Map("1" -> 2), Action.Execute),
      context = "context",
      externalId = None,
      source = JobDetails.Source.Http,
      status = JobDetails.Status.Started,
      workerId = "workerId"
    )

    it("should stop job") {
      val workerManager = TestProbe()
      val statusService = TestProbe()

      val service = new JobService(workerManager.ref, statusService.ref)

      val future = service.stopJob("id")

      statusService.expectMsgType[GetById]
      statusService.reply(Some(mkDetails(JobDetails.Status.Started)))

      workerManager.expectMsgType[CancelJobCommand]
      workerManager.reply(())

      statusService.expectMsgType[GetById]
      statusService.reply(Some(mkDetails(JobDetails.Status.Canceled)))

      val details = Await.result(future, Duration.Inf)
      details.get.status shouldBe JobDetails.Status.Canceled
    }


    def mkDetails(status: JobDetails.Status): JobDetails = {
      JobDetails(
        function = "endpoint",
        jobId = "jobId",
        params = JobParams("path", "class", Map("1" -> 2), Action.Execute),
        context = "context",
        externalId = None,
        source = JobDetails.Source.Http,
        status = status,
        workerId = "workerId"
      )
    }
  }



  def mkRunReq(id: String): RunJobRequest = {
    RunJobRequest(id, JobParams("path", "class", Map("1" -> 2), Action.Execute))
  }
}
