package io.hydrosphere.mist.master.execution

import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.execution.workers.WorkerConnection
import io.hydrosphere.mist.master.{ActorSpec, TestData, TestUtils}
import mist.api.data.{JsLikeData, JsLikeNumber}
import org.scalatest.Matchers

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

class JobActorSpec extends ActorSpec("worker-conn") with TestData with TestUtils with Matchers {

  it("should execute") {
    val callback = TestProbe()
    val promise = Promise[JsLikeData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val connectionRef = TestProbe()
    val connection = WorkerConnection("id", connectionRef.ref, workerLinkData, Promise[Unit].future)
    probe.send(actor, JobActor.Event.Perform(connection))
    connectionRef.expectMsgType[RunJobRequest]
    connectionRef.send(actor, JobStarted("id"))
    connectionRef.send(actor, JobSuccess("id", JsLikeNumber(42)))

    val result = Await.result(promise.future, Duration.Inf)
    result shouldBe JsLikeNumber(42)

    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[WorkerAssigned],
      classOf[StartedEvent],
      classOf[FinishedEvent]
    )
  }

  it("should be failed") {
    val callback = TestProbe()
    val promise = Promise[JsLikeData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val connectionRef = TestProbe()
    val connection = WorkerConnection("id", connectionRef.ref, workerLinkData, Promise[Unit].future)
    probe.send(actor, JobActor.Event.Perform(connection))
    connectionRef.expectMsgType[RunJobRequest]

    connectionRef.send(actor, JobStarted("id"))

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Started.type]

    connectionRef.send(actor, JobFailure("id", "failed"))

    intercept[Exception] {
      Await.result(promise.future, Duration.Inf)
    }
    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[WorkerAssigned],
      classOf[StartedEvent],
      classOf[FailedEvent]
    )
  }

  it("should cancel locally") {
    val callback = TestProbe()
    val promise = Promise[JsLikeData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    probe.send(actor, JobActor.Event.Cancel)
    probe.expectMsgType[JobIsCancelled]

    intercept[Exception] {
      Await.result(promise.future, Duration.Inf)
    }
    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[CanceledEvent]
    )
  }

  it("should cancel remotely") {
    val callback = TestProbe()
    val promise = Promise[JsLikeData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val connectionRef = TestProbe()
    val connection = WorkerConnection("id", connectionRef.ref, workerLinkData, Promise[Unit].future)
    probe.send(actor, JobActor.Event.Perform(connection))
    connectionRef.expectMsgType[RunJobRequest]

    connectionRef.send(actor, JobStarted("id"))

    probe.send(actor, JobActor.Event.Cancel)

    connectionRef.expectMsgType[CancelJobRequest]
    connectionRef.send(actor, JobIsCancelled("id"))

    probe.expectMsgType[JobIsCancelled]

    intercept[Exception] {
      Await.result(promise.future, Duration.Inf)
    }
    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[WorkerAssigned],
      classOf[StartedEvent],
      classOf[CanceledEvent]
    )
  }

  it("should handle connection termination") {
    val callback = TestProbe()
    val promise = Promise[JsLikeData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val connTerm = Promise[Unit]
    val connectionRef = TestProbe()
    val connection = WorkerConnection("id", connectionRef.ref, workerLinkData, connTerm.future)
    probe.send(actor, JobActor.Event.Perform(connection))
    connectionRef.expectMsgType[RunJobRequest]

    connectionRef.send(actor, JobStarted("id"))

    connTerm.success(())

    intercept[Exception] {
      Await.result(promise.future, Duration.Inf)
    }
    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[WorkerAssigned],
      classOf[StartedEvent],
      classOf[FailedEvent]
    )
  }

  class TestReporter extends StatusReporter {

    private var reported = Vector.empty[Class[_ <: UpdateStatusEvent]]

    def report(ev: UpdateStatusEvent): Unit =
      reported = reported :+ ev.getClass

    def containsExactly(events: Class[_ <: UpdateStatusEvent]*): Unit =
      reported should contain theSameElementsInOrderAs events
  }

  object TestReporter {
    def apply(): TestReporter = new TestReporter()
  }
}
