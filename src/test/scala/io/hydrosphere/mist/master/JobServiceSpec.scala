package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages.{CancelJobRequest, JobParams, RunJobRequest}
import io.hydrosphere.mist.Messages.StatusMessages
import io.hydrosphere.mist.Messages.StatusMessages.Register
import io.hydrosphere.mist.Messages.WorkerMessages.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.jobs.{Action, JobDetails}
import io.hydrosphere.mist.master.models.{JobStartRequest, RunMode, EndpointConfig, RunSettings}
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
          endpoint = EndpointConfig("name", "path", "className", "context"),
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
      endpoint = "endpoint",
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

      statusService.expectMsgType[StatusMessages.GetById]
      statusService.reply(Some(mkDetails(JobDetails.Status.Started)))

      workerManager.expectMsgType[CancelJobCommand]
      workerManager.reply(())

      statusService.expectMsgType[StatusMessages.GetById]
      statusService.reply(Some(mkDetails(JobDetails.Status.Canceled)))

      val details = Await.result(future, Duration.Inf)
      details.get.status shouldBe JobDetails.Status.Canceled
    }


    def mkDetails(status: JobDetails.Status): JobDetails = {
      JobDetails(
        endpoint = "endpoint",
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
