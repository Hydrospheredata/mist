package io.hydrosphere.mist.utils.akka

import _root_.akka.actor._
import _root_.akka.testkit.{TestKit, TestProbe}
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration._

class ActorRegSpec extends TestKit(ActorSystem("actorReg"))
  with FunSpecLike
  with Matchers {

  it("should wait registration") {
    val hub = ActorRegHub("x1", system)
    val probe = TestProbe()

    val waiterRef = {
      val selection = system.actorSelection(hub.regPath)
      Await.result(selection.resolveOne(1 second), Duration.Inf)
    }

    val f = hub.waitRef("id", Duration.Inf)
    probe.send(waiterRef, ActorRegHub.Register("id"))

    val registered = Await.result(f, Duration.Inf)
    registered shouldBe probe.ref
  }

  it("should be failed by timeout") {
    val hub = ActorRegHub("x2", system)
    val f = hub.waitRef("id", 2.seconds)
    intercept[Exception] {
      Await.result(f, Duration.Inf)
    }
  }
}
