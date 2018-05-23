package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.AtomicBoolean

import akka.testkit.TestProbe
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.master.execution.workers.WorkerBridge.Event.CompleteAndShutdown
import io.hydrosphere.mist.master.{ActorSpec, TestData, TestUtils}
import mist.api.data._

import scala.concurrent.Promise
import scala.concurrent.duration._

class WorkerBridgeSpec extends ActorSpec("worker-conn") with TestData with TestUtils {

  it("should init and watch worker") {
    val remote = TestProbe()

    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 minute, promise, remote.ref, StopAction.Remote)
    val bridge = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(bridge, WorkerReady("id", None))

    val connection = promise.future.await(1 second)
    connection.ref shouldBe bridge
    connection.id shouldBe "id"

    system.stop(remote.ref)
    shouldTerminate(1 second)(bridge)
  }

  it("should be failed by timeout") {
    val remote = TestProbe()
    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 second, promise, remote.ref, StopAction.Remote)

    val bridge = system.actorOf(props)

    intercept[Exception] {
      promise.future.await(3 second)
    }
    shouldTerminate(1 second)(bridge)
  }

  it("should invoke remote stop action") {
    val remote = TestProbe()

    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 minute, promise, remote.ref, StopAction.Remote)
    val bridge = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(bridge, WorkerReady("id", None))

    val connection = promise.future.await(1 second)
    connection.ref shouldBe bridge
    connection.id shouldBe "id"

    bridge ! WorkerBridge.Event.ForceShutdown
    remote.expectMsgType[ShutdownWorker.type]
    bridge ! RequestTermination
    remote.expectMsgType[ShutdownWorkerApp.type]
    bridge ! Goodbye
    shouldTerminate(1 second)(bridge)
  }

  it("should invoke custom stop action") {
    val check = new AtomicBoolean(false)
    val stopAction = StopAction.CustomFn(id => {
      check.set(true)
    })
    val remote = TestProbe()

    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 minute, promise, remote.ref, stopAction)
    val bridge = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(bridge, WorkerReady("id", None))

    val connection = promise.future.await(1 second)
    connection.ref shouldBe bridge
    connection.id shouldBe "id"

    bridge ! WorkerBridge.Event.ForceShutdown
    remote.expectMsgType[ShutdownWorker.type]
    bridge ! RequestTermination
    bridge ! Goodbye
    shouldTerminate(1 second)(bridge)
    check.get() shouldBe true
  }

  it("should correctly proxy events") {
    val remote = TestProbe()

    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 minute, promise, remote.ref, StopAction.Remote)
    val bridge = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(bridge, WorkerReady("id", None))

    val connection = promise.future.await(1 second)
    connection.ref shouldBe bridge
    connection.id shouldBe "id"

    val jobProbe = TestProbe()
    jobProbe.send(connection.ref, mkRunReq("id"))
    remote.expectMsgType[RunJobRequest]

    remote.send(connection.ref, JobStarted("id"))
    remote.send(connection.ref, JobFileDownloading("id"))
    jobProbe.expectMsgType[JobStarted]
    jobProbe.expectMsgType[JobFileDownloading]

    jobProbe.send(connection.ref, CancelJobRequest("id"))
    remote.expectMsgType[CancelJobRequest]
    remote.send(connection.ref, JobIsCancelled("id"))

    jobProbe.expectMsgType[JobIsCancelled]

    remote.send(connection.ref, JobSuccess("id", JsMap.empty))
    jobProbe.expectMsgType[JobSuccess]
  }

  it("should complete and shutdown") {
    val remote = TestProbe()

    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 minute, promise, remote.ref, StopAction.Remote)
    val bridge = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(bridge, WorkerReady("id", None))

    val connection = promise.future.await(1 second)
    connection.ref shouldBe bridge
    connection.id shouldBe "id"

    val jobProbe = TestProbe()
    jobProbe.send(connection.ref, mkRunReq("id"))
    remote.expectMsgType[RunJobRequest]
    connection.ref ! CompleteAndShutdown

    remote.send(connection.ref, JobSuccess("id", JsMap.empty))
    jobProbe.expectMsgType[JobSuccess]

    remote.expectMsgType[ShutdownWorker.type]
    bridge ! RequestTermination
    remote.expectMsgType[ShutdownWorkerApp.type]
    bridge ! Goodbye
    shouldTerminate(1 second)(bridge)
  }
}
