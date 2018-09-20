package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorRef, Props}
import akka.testkit.{TestActorRef, TestProbe}
import io.hydrosphere.mist.master.ActorSpec

import scala.concurrent.{Future, Promise}

class FutureSubscribeSpec extends ActorSpec("future-subsribe-spec") {

  import FutureSubscribeSpec._

  it("should handle success") {
    val actor = TestActorRef[TestActor](Props(classOf[TestActor]))
    val probe = TestProbe()

    val p = Promise[Unit]
    probe.send(actor, TestMessage(p.future))
    p.success(())
    probe.expectMsgType[Ok.type]
  }

  it("should handle failure") {
    val actor = TestActorRef[TestActor](Props(classOf[TestActor]))
    val probe = TestProbe()

    val p = Promise[Unit]
    probe.send(actor, TestMessage(p.future))
    p.failure(new RuntimeException())
    probe.expectMsgType[Err.type]
  }

}
object FutureSubscribeSpec {

  sealed trait Rsp
  case object Ok extends Rsp
  case object Err extends Rsp
  case class TestMessage(future: Future[Unit])

  class TestActor extends Actor with FutureSubscribe {

    import context._

    override def receive: Receive = {
      case TestMessage(future) =>
        subscribe0(future)(_ => Ok, _ => Err)
        context become respond(sender())
    }

    private def respond(respond: ActorRef): Receive = {
      case x: Rsp => respond ! x
    }

  }

}
