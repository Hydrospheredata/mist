package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event.Released
import io.hydrosphere.mist.master.{ActorSpec, TestData}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class SharedConnectorSpec extends ActorSpec("shared-conn") with Matchers with TestData with Eventually {


  it("should start connection 'til max jobs connections started") {
    val callCounter = new AtomicInteger(0)

    val remote = TestProbe()
    val connector = TestActorRef[SharedConnector](SharedConnector.props(
      id = "id",
      ctx = FooContext.copy(maxJobs = 2),
      startConnection = (id, ctx) => {
        val x = callCounter.incrementAndGet()
        val conn = WorkerConnection(x.toString, remote.ref, workerLinkData, Promise[Unit].future)
        Future.successful(conn)
      }
    ))

    val probe = TestProbe()

    val resolve = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))
    val connection1 = Await.result(resolve.future, Duration.Inf)

    val resolve2 = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve2))
    val connection2 = Await.result(resolve2.future, Duration.Inf)
    val resolve3 = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve3))

    connection1.id shouldBe "1"
    connection2.id shouldBe "2"

    connection1.release()
    val conn3 = Await.result(resolve3.future, Duration.Inf)
    conn3.id shouldBe "1"
    callCounter.get() shouldBe 2
  }

  it("should warmup fully") {
    val callCounter = new AtomicInteger(0)

    val remote = TestProbe()
    val connector = TestActorRef[SharedConnector](SharedConnector.props(
      id = "id",
      ctx = FooContext,
      startConnection = (id, ctx) => {
        val x = callCounter.incrementAndGet()
        val conn = WorkerConnection(x.toString, remote.ref, workerLinkData, Promise[Unit].future)
        Future.successful(conn)
      }
    ))

    val probe = TestProbe()
    probe.send(connector, WorkerConnector.Event.WarmUp)
    callCounter.get() shouldBe 2
    probe.send(connector, WorkerConnector.Event.GetStatus)
    val status = probe.expectMsgType[SharedConnector.ProcessStatus]
    status.poolSize shouldBe 2
    status.requestsSize shouldBe 0
    status.inUseSize shouldBe 0
    status.startingConnections shouldBe 0
  }

  it("should acquire connection from pool when it released with no requests") {
    val callCounter = new AtomicInteger(0)

    val remote = TestProbe()
    val connector = TestActorRef[SharedConnector](SharedConnector.props(
      id = "id",
      ctx = FooContext,
      startConnection = (id, ctx) => {
        val x = callCounter.incrementAndGet()
        val conn = WorkerConnection(x.toString, remote.ref, workerLinkData, Promise[Unit].future)
        Future.successful(conn)
      }
    ))

    val probe = TestProbe()

    val resolve = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))
    val connection1 = Await.result(resolve.future, Duration.Inf)

    val resolve2 = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve2))
    val connection2 = Await.result(resolve2.future, Duration.Inf)

    connection1.id shouldBe "1"
    connection2.id shouldBe "2"

    connection1.release()

    val resolve3 = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve3))

    val conn3 = Await.result(resolve3.future, Duration.Inf)
    conn3.id shouldBe "1"
  }

  it("should watch connection when in use") {
    val termination = Promise[Unit]
    val connector = TestActorRef[SharedConnector](SharedConnector.props(
      id = "id",
      ctx = FooContext,
      startConnection = (_, _) => Future.successful(WorkerConnection("id", TestProbe().ref, workerLinkData, termination.future))
    ))

    val probe = TestProbe()

    val resolve = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))
    val connection1 = Await.result(resolve.future, Duration.Inf)
    probe.send(connector, WorkerConnector.Event.GetStatus)
    val status = probe.expectMsgType[SharedConnector.ProcessStatus]
    status.inUseSize shouldBe 1
    status.poolSize shouldBe 0

    termination.success(())
    //ConnTerminated is fired
    probe.send(connector, WorkerConnector.Event.GetStatus)
    val status2 = probe.expectMsgType[SharedConnector.ProcessStatus]
    status2.inUseSize shouldBe 0
    status2.poolSize shouldBe 0
  }

  it("should watch connection when in pool") {
    val termination = Promise[Unit]
    val connector = TestActorRef[SharedConnector](SharedConnector.props(
      id = "id",
      ctx = FooContext,
      startConnection = (_, _) => Future.successful(WorkerConnection("id", TestProbe().ref, workerLinkData, termination.future))
    ))

    val probe = TestProbe()

    val resolve = Promise[PerJobConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))
    val connection1 = Await.result(resolve.future, Duration.Inf)

    probe.send(connector, WorkerConnector.Event.GetStatus)
    val status = probe.expectMsgType[SharedConnector.ProcessStatus]
    status.inUseSize shouldBe 1
    status.poolSize shouldBe 0

    connection1.release()

    probe.send(connector, WorkerConnector.Event.GetStatus)
    val status1 = probe.expectMsgType[SharedConnector.ProcessStatus]
    status1.poolSize shouldBe 1
    status1.inUseSize shouldBe 0

    termination.success(())

    //ConnTerminated is fired
    probe.send(connector, WorkerConnector.Event.GetStatus)
    val status2 = probe.expectMsgType[SharedConnector.ProcessStatus]
    status2.poolSize shouldBe 0
    status2.inUseSize shouldBe 0
  }

  describe("Shared conn wrapper") {

    it("should release connection") {
      val connRef = TestProbe()
      val termination = Promise[Unit]
      val connection = WorkerConnection(
        id = "id",
        ref = connRef.ref,
        data = workerLinkData,
        whenTerminated = termination.future
      )
      val connector = TestProbe()
      val wrapped = SharedConnector.wrappedConnection(connector.ref, connection)
      wrapped.release()
      connector.expectMsgType[Released]
      connRef.expectNoMessage(1 second)

      wrapped.run(mkRunReq("id"), ActorRef.noSender)
      connRef.expectMsgType[RunJobRequest]
    }
  }

}
