package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.AtomicBoolean

import akka.testkit.TestProbe
import io.hydrosphere.mist.core.CommonData.{ForceShutdown, WorkerInitInfo, WorkerReady}
import io.hydrosphere.mist.master.{ActorSpec, TestData, TestUtils}

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

    bridge ! WorkerBridge.Shutdown
    remote.expectMsgType[ForceShutdown.type]
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

    bridge ! WorkerBridge.Shutdown
    shouldTerminate(1 second)(bridge)
    check.get() shouldBe true
  }

}
