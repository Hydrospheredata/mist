package io.hydrosphere.mist.master.execution

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.execution.status.{ReportedEvent, StatusReporter}
import io.hydrosphere.mist.master.execution.workers.{PerJobConnection, WorkerConnection}
import io.hydrosphere.mist.master.logging.JobLogger
import io.hydrosphere.mist.master.{ActorSpec, JobDetails, TestData, TestUtils}
import mist.api.data.{JsData, JsNumber}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.mockito.Mockito._

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._

class JobActorSpec extends ActorSpec("worker-conn")
  with TestData
  with TestUtils
  with Matchers
  with Eventually
  with MockitoSugar {

  it("should execute") {
    val callback = TestProbe()
    val promise = Promise[JsData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter,
      jobLogger = JobLogger.NOOP
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val conn = mockConn()
    probe.send(actor, JobActor.Event.Perform(conn))
    eventually(timeout(Span(3, Seconds))) {
      verify(conn).run(any[RunJobRequest], any[ActorRef])
    }

    val connectionRef = TestProbe()
    connectionRef.send(actor, JobStarted("id"))
    connectionRef.send(actor, JobSuccess("id", JsNumber(42)))

    val result = Await.result(promise.future, 5 seconds)
    result shouldBe JsNumber(42)

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
    val promise = Promise[JsData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter,
      jobLogger = JobLogger.NOOP
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val conn = mockConn()
    probe.send(actor, JobActor.Event.Perform(conn))
    eventually(timeout(Span(3, Seconds))) {
      verify(conn).run(any[RunJobRequest], any[ActorRef])
    }

    val connectionRef = TestProbe()
    connectionRef.send(actor, JobStarted("id"))

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Started.type]

    connectionRef.send(actor, JobFailure("id", "failed"))

    intercept[Exception] {
      Await.result(promise.future, 5 seconds)
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
    val promise = Promise[JsData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter,
      jobLogger = JobLogger.NOOP
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    probe.send(actor, JobActor.Event.Cancel)
    probe.expectMsgType[ContextEvent.JobCancelledResponse]

    intercept[Exception] {
      Await.result(promise.future, 5 seconds)
    }
    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[CancellingEvent],
      classOf[CancelledEvent]
    )
  }

  it("should cancel remotely") {
    val callback = TestProbe()
    val promise = Promise[JsData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter,
      jobLogger = JobLogger.NOOP
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val conn = mockConn()
    probe.send(actor, JobActor.Event.Perform(conn))
    eventually(timeout(Span(3, Seconds))) {
      verify(conn).run(any[RunJobRequest], any[ActorRef])
    }

    val connectionRef = TestProbe()
    connectionRef.send(actor, JobStarted("id"))

    probe.send(actor, JobActor.Event.Cancel)

    eventually(timeout(Span(3, Seconds))) {
      verify(conn).cancel(any[String], any[ActorRef])
    }
    connectionRef.send(actor, JobIsCancelled("id"))
    connectionRef.send(actor, JobFailure("id", "error"))

    probe.expectMsgType[ContextEvent.JobCancelledResponse]

    intercept[Exception] {
      Await.result(promise.future, 5 seconds)
    }
    callback.expectMsgType[JobActor.Event.Completed]
    shouldTerminate(1 second)(actor)

    reporter.containsExactly(
      classOf[QueuedEvent],
      classOf[WorkerAssigned],
      classOf[StartedEvent],
      classOf[CancellingEvent],
      classOf[CancelledEvent]
    )
  }

  it("should handle connection termination") {
    val callback = TestProbe()
    val promise = Promise[JsData]

    val reporter = TestReporter()
    val actor = TestActorRef[JobActor](JobActor.props(
      callback = callback.ref,
      req = mkRunReq("id"),
      promise = promise,
      reporter = reporter,
      jobLogger = JobLogger.NOOP
    ))

    val probe = TestProbe()

    probe.send(actor, JobActor.Event.GetStatus)
    probe.expectMsgType[ExecStatus.Queued.type]

    val connTerm = Promise[Unit]

    val conn = mockConn(term = connTerm)
    probe.send(actor, JobActor.Event.Perform(conn))
    eventually(timeout(Span(3, Seconds))) {
      verify(conn).run(any[RunJobRequest], any[ActorRef])
    }

    val connectionRef = TestProbe()
    connectionRef.send(actor, JobStarted("id"))

    connTerm.success(())

    intercept[Exception] {
      Await.result(promise.future, 5 seconds)
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

  def mockConn(id: String = "id", term: Promise[Unit] = Promise[Unit]): PerJobConnection = {
    val m = mock[PerJobConnection]
    when(m.id).thenReturn(id)
    when(m.whenTerminated).thenReturn(term.future)
    m
  }

  class TestReporter extends StatusReporter {

    private var reported = Vector.empty[Class[_ <: UpdateStatusEvent]]

    def containsExactly(events: Class[_ <: UpdateStatusEvent]*): Unit =
      reported should contain theSameElementsInOrderAs events

    override def report(ev: ReportedEvent): Unit = {
      reported = reported :+ ev.e.getClass
      ev match {
        case ReportedEvent.FlushCallback(_, callback) => callback.success(mkDetails(JobDetails.Status.Canceled))
        case _ =>
      }
    }
  }

  object TestReporter {
    def apply(): TestReporter = new TestReporter()
  }
}
