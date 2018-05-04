package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData.RunJobRequest
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
    val resolve = Promise[PerJobConnection]
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
    val resolve = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))

    val connection = Await.result(resolve.future, Duration.Inf)

    connection.run(mkRunReq("id"), probe.ref)

    originalRef.expectMsgType[RunJobRequest]
    originalRef.expectMsgType[WorkerBridge.Event.CompleteAndShutdown.type]
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
      val wrapped = ExclusiveConnector.wrappedConn(connection)

      wrapped.release()
      connRef.expectMsgType[WorkerBridge.Event.CompleteAndShutdown.type]

      wrapped.run(mkRunReq("id"), ActorRef.noSender)
      connRef.expectMsgType[RunJobRequest]
    }
  }
}
