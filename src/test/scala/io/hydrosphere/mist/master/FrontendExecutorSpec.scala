package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}
import io.hydrosphere.mist.Messages.JobMessages._
import io.hydrosphere.mist.Messages.WorkerMessages.WorkerUp
import io.hydrosphere.mist.jobs.{JobDetails, Action}
import org.scalatest.{Matchers, FunSpecLike}

class FrontendExecutorSpec extends TestKit(ActorSystem("testFront"))
  with FunSpecLike
  with Matchers {

  describe("without worker") {

    it("should queue jobs") {
      val probe = TestProbe()
      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5))

      probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

      probe.expectMsgPF(){
        case info: ExecutionInfo =>
          info.request.id shouldBe "id"
          info.status shouldBe JobDetails.Status.Queued
      }
    }

    it("should cancel jobs") {

      val probe = TestProbe()
      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5))

      probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))
      probe.expectMsgType[ExecutionInfo]

      probe.send(frontend, CancelJobRequest("id"))
      probe.expectMsgType[JobIsCancelled]
    }
  }

  describe("with worker") {

    it("should send jobs to worker") {
      val probe = TestProbe()
      val backend = TestProbe()

      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5))

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

      val frontend = system.actorOf(FrontendJobExecutor.props("test", 5))

      frontend ! WorkerUp(backend.ref)

      probe.send(frontend, RunJobRequest("id", JobParams("path", "MyClass", Map.empty, Action.Execute)))

      probe.expectMsgType[ExecutionInfo]
      backend.expectMsgType[RunJobRequest]

      backend.reply(JobStarted("id"))

      probe.send(frontend, CancelJobRequest("id"))
      backend.expectMsgType[CancelJobRequest]
      backend.reply(JobIsCancelled("id"))
    }
  }
}
