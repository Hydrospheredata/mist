package io.hydrosphere.mist.master

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

import scala.concurrent.duration.FiniteDuration

abstract class ActorSpec(name: String) extends TestKit(ActorSystem(name))
  with FunSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    system.terminate()
  }

  def shouldTerminate(f: FiniteDuration)(ref: ActorRef): Unit = {
    val probe = TestProbe()
    probe.watch(ref)
    probe.expectTerminated(ref, f)
  }
}
