package io.hydrosphere.mist.master.execution.workers

import io.hydrosphere.mist.master.TestData
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Promise

class ConnectionsMirrorSpec extends FunSpec with Matchers with TestData {

  it("should collect connections") {
    def mkConnection(id: String): WorkerConnection = {
      WorkerConnection(
        id = id,
        ref = null,
        data = workerLinkData.copy(name = id),
        whenTerminated = Promise[Unit].future
      )
    }
    val mirror = ConnectionsMirror.standalone()

    mirror.workerConnections().isEmpty shouldBe true
    (1 to 10).foreach(i => mirror.add(mkConnection(i.toString)))
    mirror.workerConnections().size shouldBe 10
    mirror.workerConnection("1").isDefined shouldBe true

    mirror.remove("1")
    mirror.workerConnection("1").isDefined shouldBe false
  }
}
