package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.StatusMessages.{QueuedEvent, CanceledEvent}
import io.hydrosphere.mist.Messages.WorkerMessages.WorkerUp
import io.hydrosphere.mist.jobs.{JobDetails, Action}
import org.scalatest.{Matchers, FunSpecLike}

class FrontendExecutorSpec extends TestKit(ActorSystem("testFront"))
  with FunSpecLike
  with Matchers {

  val StatusService = TestProbe().ref
  describe("without worker") {

    it("should queue jobs") {
      val probe = TestProbe()
      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5, StatusService))

      probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

      probe.expectMsgPF(){
        case info: ExecutionInfo =>
          info.request.id shouldBe "id"
          info.status shouldBe JobDetails.Status.Queued
      }
    }

    it("should cancel jobs") {
      val probe = TestProbe()
      val status = TestProbe()
      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5, status.ref))

      val params = JobParams("path", "MyClass", Map.empty, Action.Execute)

      probe.send(frontend, RunJobRequest("id", params))
      probe.expectMsgType[ExecutionInfo]

      probe.send(frontend, CancelJobRequest("id"))

      status.expectMsgType[QueuedEvent]
      status.expectMsgType[CanceledEvent]
      status.reply(JobDetails("endp", "id", params, "context", None, JobDetails.Source.Http, workerId = "workerId"))

      probe.expectMsgType[JobIsCancelled]
    }
  }

  describe("with worker") {

    it("should send jobs to worker") {
      val probe = TestProbe()
      val backend = TestProbe()

      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5, StatusService))

      frontend ! WorkerUp(backend.ref)

      probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

      probe.expectMsgPF() {
        case info: ExecutionInfo =>
          info.request.id shouldBe "id"
          info.status shouldBe JobDetails.Status.Queued
      }

      backend.expectMsgType[RunJobRequest]
    }

    it("should cancel jobs on worker") {
      val probe = TestProbe()
      val backend = TestProbe()

      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5, StatusService))

      frontend ! WorkerUp(backend.ref)

      probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

      probe.expectMsgType[ExecutionInfo]
      backend.expectMsgType[RunJobRequest]

      backend.reply(JobStarted("id"))

      probe.send(frontend, CancelJobRequest("id"))
      backend.expectMsgType[CancelJobRequest]
      backend.reply(JobIsCancelled("id"))
    }

    it("should control queue of jobs in complex case") {
      val probe = TestProbe()
      val backend = TestProbe()

      val frontend = system.actorOf(FrontendJobExecutor.props("test", 1, StatusService))

      frontend ! WorkerUp(backend.ref)

      val req1 = RunJobRequest("1", JobParams("path", "MyClass", Map.empty, Action.Execute))
      val req2 = RunJobRequest("2", JobParams("path", "MyClass", Map.empty, Action.Execute))

      probe.send(frontend, req1)
      probe.send(frontend, req2)

      backend.expectMsg(req1)
      backend.reply(JobStarted("1"))
      backend.reply(JobSuccess("1", Map("result" -> "1")))

      backend.expectMsg(req2)
      backend.reply(JobStarted("2"))
      backend.reply(JobSuccess("2", Map("result" -> "2")))
      backend.expectNoMsg()

      probe.expectMsgType[ExecutionInfo]
      probe.expectMsgType[ExecutionInfo]

      val req3 = RunJobRequest("3", JobParams("path", "MyClass", Map.empty, Action.Execute))
      probe.send(frontend, req3)
      backend.expectMsg(req3)
    }
  }
}
