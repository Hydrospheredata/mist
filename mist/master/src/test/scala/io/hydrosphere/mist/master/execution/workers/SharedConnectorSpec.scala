package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event.ConnTerminated
import io.hydrosphere.mist.master.{ActorSpec, TestData}
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class SharedConnectorSpec extends ActorSpec("shared-conn") with Matchers with TestData with Eventually {


  it("should start connection 'til max jobs connections started") {
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

    val resolve = Promise[WorkerConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))
    val connection1 = Await.result(resolve.future, Duration.Inf)

    val resolve2 = Promise[WorkerConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve2))
    val connection2 = Await.result(resolve2.future, Duration.Inf)
    val resolve3 = Promise[WorkerConnection]
    probe.send(connector, WorkerConnector.Event.AskConnection(resolve3))

    connection1.id shouldBe "1"
    connection2.id shouldBe "2"


    probe.send(connector, WorkerConnector.Event.ReleaseConnection(connection1.id))
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
    probe.send(connector, WorkerConnector.Event.WarnUp)
    callCounter.get() shouldBe 2
  }

//  it("should watch connection") {
//    val termination = Promise[Unit]
//    val connector = TestActorRef[SharedConnector](SharedConnector.props(
//      id = "id",
//      ctx = FooContext,
//      startConnection = (_, _) => Future.successful(WorkerConnection("id", TestProbe().ref, workerLinkData, termination.future))
//    ))
//
//    val probe = TestProbe()
//
//    val resolve = Promise[WorkerConnection]
//    probe.send(connector, WorkerConnector.Event.AskConnection(resolve))
//    val connection1 = Await.result(resolve.future, Duration.Inf)
//
//    termination.success(())
//
//    shouldTerminate(1 second)(connector)
//  }

  describe("Shared conn wrapper") {

    it("should ignore unused") {
      val connRef = TestProbe()
      val termination = Promise[Unit]
      val connection = WorkerConnection(
        id = "id",
        ref = connRef.ref,
        data = workerLinkData,
        whenTerminated = termination.future
      )
      val wrapped = SharedConnector.ConnectionWrapper.wrap(connection)

      wrapped.markUnused()
      connRef.expectNoMessage(1 second)

      wrapped.ref ! mkRunReq("id")
      connRef.expectMsgType[RunJobRequest]
    }
  }

}
