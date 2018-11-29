package io.hydrosphere.mist.master.execution.workers

import akka.testkit.TestProbe
import io.hydrosphere.mist.master.ActorSpec
import io.hydrosphere.mist.master.execution.Cluster
import io.hydrosphere.mist.master.execution.Cluster.ActorBasedWorkerConnector

import scala.concurrent.Promise

class ClusterSpec extends ActorSpec("actor-based-connector") {

  it("should proxy call to actor") {
    val target = TestProbe()
    val connector = new ActorBasedWorkerConnector(target.ref, Promise[Unit].future)

    connector.askConnection()
    target.expectMsgType[Cluster.Event.AskConnection]

    connector.shutdown(true)
    target.expectMsgType[Cluster.Event.Shutdown]
  }

}
