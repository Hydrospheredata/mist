package io.hydrosphere.mist.master.execution.workers

import akka.testkit.TestProbe
import io.hydrosphere.mist.core.CommonData.{WorkerInitInfo, WorkerReady}
import io.hydrosphere.mist.master.{ActorSpec, TestData, TestUtils}

import scala.concurrent.Promise
import scala.concurrent.duration._

class WorkerBridgeSpec extends ActorSpec("worker-conn") with TestData with TestUtils {

  it("should init and watch worker") {
    val remote = TestProbe()

    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 minute, promise, remote.ref)
    val connection = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(connection, WorkerReady("id", None))

    promise.future.await(1 second) shouldBe connection

    system.stop(remote.ref)
    shouldTerminate(1 second)(connection)
  }

  it("should be failed by timeout") {
    val remote = TestProbe()
    val promise = Promise[WorkerConnection]
    val props = WorkerBridge.props("id", workerInitData, 1 second, promise, remote.ref)

    val connection = system.actorOf(props)

    intercept[Exception] {
      promise.future.await(3 second)
    }
    shouldTerminate(1 second)(connection)
  }

}
