package io.hydrosphere.mist.master.execution.workers

import akka.testkit.TestProbe
import io.hydrosphere.mist.master.{ActorSpec, TestData}

import scala.concurrent.Promise

class WorkerConnectionSpec extends ActorSpec("worker_conn") with TestData {

  it("should send shutdown command") {
    val connRef = TestProbe()

    val termination = Promise[Unit]
    val connection = WorkerConnection(
      id = "id",
      ref = connRef.ref,
      data = workerLinkData,
      whenTerminated = termination.future
    )

    connection.shutdown(true)
    connRef.expectMsgType[WorkerBridge.Event.ForceShutdown.type]
    connection.shutdown(false)
    connRef.expectMsgType[WorkerBridge.Event.CompleteAndShutdown.type]
  }
}
