package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.ActorSystem
import akka.testkit.{TestProbe, TestKit}
import io.hydrosphere.mist.Messages.ListRoutes
import io.hydrosphere.mist.Messages.WorkerMessages.StopAllWorkers
import io.hydrosphere.mist.jobs.JobDefinition
import io.hydrosphere.mist.master.JobRoutes
import org.scalatest.{Matchers, FunSpecLike}
import org.mockito.Mockito._
import scala.concurrent.duration._

class CliResponderSpec extends TestKit(ActorSystem("cliResponderTest"))
  with FunSpecLike
  with Matchers {

  it("should return routes list") {
    val jobRoutes = mock(classOf[JobRoutes])
    when(jobRoutes.listDefinition())
      .thenReturn(Seq(
        JobDefinition("first", "jar.jar", "MyClass", "namespace"),
        JobDefinition("second", "py.py", "MyClass", "namespace")
      ))


    val responder = system.actorOf(CliResponder.props(jobRoutes, TestProbe().ref))

    val probe = TestProbe()
    probe.send(responder, ListRoutes)

    val msg = probe.receiveOne(1.second)
    val definitions = msg.asInstanceOf[List[JobDefinition]]
    definitions.size shouldBe 2
  }

  it("should forward other messages to manager") {
    val jobRoutes = mock(classOf[JobRoutes])
    val manager = TestProbe()
    val responder = system.actorOf(CliResponder.props(jobRoutes, manager.ref))

    val probe = TestProbe()
    probe.send(responder, StopAllWorkers)

    manager.expectMsg(StopAllWorkers)
  }
}
