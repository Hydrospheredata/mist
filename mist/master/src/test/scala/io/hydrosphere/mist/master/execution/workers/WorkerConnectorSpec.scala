package io.hydrosphere.mist.master.execution.workers

import akka.testkit.TestProbe
import io.hydrosphere.mist.master.ActorSpec
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.ActorBasedWorkerConnector

import scala.concurrent.Promise

class WorkerConnectorSpec extends ActorSpec("actor-based-connector") {

  it("should proxy call to actor") {
    val target = TestProbe()
    val connector = new ActorBasedWorkerConnector(target.ref, Promise[Unit].future)

    connector.askConnection()
    target.expectMsgType[WorkerConnector.Event.AskConnection]

    connector.warmUp()
    target.expectMsgType[WorkerConnector.Event.WarmUp.type]

    connector.shutdown(true)
    target.expectMsgType[WorkerConnector.Event.Shutdown]
  }

}
