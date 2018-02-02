package io.hydrosphere.mist.master.execution.remote

import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.core.CommonData.{WorkerInitInfo, WorkerReady}
import io.hydrosphere.mist.master.{ActorSpec, TestData, TestUtils}

import scala.concurrent.Promise
import scala.concurrent.duration._

class WorkerConnectionSpec extends ActorSpec("worker-conn") with TestData with TestUtils {

  it("should init and watch worker") {
    val remote = TestProbe()

    val promise = Promise[Unit]
    val props = WorkerConnection.props("id", workerInitData, remote.ref, promise, 1 minute)
    val connection = system.actorOf(props)

    remote.expectMsgType[WorkerInitInfo]
    remote.send(connection, WorkerReady("id"))

    promise.future.await(1 second) shouldBe ()

    system.stop(remote.ref)
    shouldTerminate(1 second)(connection)
  }

  it("should be failed by timeout") {
    val remote = TestProbe()
    val promise = Promise[Unit]
    val props = WorkerConnection.props("id", workerInitData, remote.ref, promise, 1 second)

    val connection = system.actorOf(props)

    intercept[Exception] {
      promise.future.await(3 second)
    }
    shouldTerminate(1 second)(connection)
  }

}
