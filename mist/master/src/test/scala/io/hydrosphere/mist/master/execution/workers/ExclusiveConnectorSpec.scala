package io.hydrosphere.mist.master.execution.workers

import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData.{CompleteAndShutdown, RunJobRequest}
import io.hydrosphere.mist.master.{ActorSpec, TestData}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

class ExclusiveConnectorSpec extends ActorSpec("excl-conn") with TestData {

  it("shouldn't ignore errors") {
    val connector = TestActorRef[ExclusiveConnector](ExclusiveConnector.props(
      id = "id",
      ctx = FooContext,
      startWorker = (_, _) => Future.failed(new RuntimeException("err"))
    ))

    val probe = TestProbe()
    val resolve = Promise[WorkerConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))

    intercept[Throwable] {
      Await.result(resolve.future, Duration.Inf)
    }
  }

  it("should return wrapped connections") {
    val originalRef = TestProbe()
    val original = WorkerConnection("id", originalRef.ref, workerLinkData, Promise[Unit].future)

    val connector = TestActorRef[ExclusiveConnector](ExclusiveConnector.props(
      id = "id",
      ctx = FooContext,
      startWorker = (_, _) => Future.successful(original)
    ))

    val probe = TestProbe()
    val resolve = Promise[WorkerConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))

    val connection = Await.result(resolve.future, Duration.Inf)

    probe.send(connection.ref, mkRunReq("id"))

    originalRef.expectMsgType[RunJobRequest]
    originalRef.expectMsgType[CompleteAndShutdown.type]
  }

  describe("Exclusive conn wrapper") {

    it("should handle unused") {
      val connRef = TestProbe()
      val termination = Promise[Unit]
      val connection = WorkerConnection(
        id = "id",
        ref = connRef.ref,
        data = workerLinkData,
        whenTerminated = termination.future
      )
      val wrapped = ExclusiveConnector.ConnectionWrapper.wrap(connection)

      wrapped.markUnused()
      connRef.expectMsgType[CompleteAndShutdown.type]

      wrapped.ref ! mkRunReq("id")
      connRef.expectMsgType[RunJobRequest]
    }
  }
}
