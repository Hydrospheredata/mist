package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.Messages.ListRoutes
import io.hydrosphere.mist.Messages.StatusMessages.RunningJobs
import io.hydrosphere.mist.Messages.WorkerMessages.StopAllWorkers
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.{JobDefinition, JobDetails, JobExecutionParams}
import io.hydrosphere.mist.master.{JobRoutes, MasterService}
import org.scalatest.{FunSpecLike, Matchers}
import org.mockito.Mockito._

import scala.concurrent.Future
import scala.concurrent.duration._

class CliResponderSpec extends TestKit(ActorSystem("cliResponderTest"))
  with FunSpecLike
  with Matchers {

  it("should return routes list") {
    val master = mock(classOf[MasterService])
    when(master.routeDefinitions())
      .thenReturn(Seq(
        JobDefinition("first", "jar.jar", "MyClass", "namespace"),
        JobDefinition("second", "py.py", "MyClass", "namespace")
      ))


    val responder = system.actorOf(CliResponder.props(master, TestProbe().ref))

    val probe = TestProbe()
    probe.send(responder, ListRoutes)

    val msg = probe.receiveOne(1.second)
    val definitions = msg.asInstanceOf[List[JobDefinition]]
    definitions.size shouldBe 2
  }

  it("should return running jobs") {
    val master = mock(classOf[MasterService])
    when(master.activeJobs())
      .thenReturn(Future.successful(List(
        JobDetails(
          configuration = JobExecutionParams("path", "MyClass", "namespace", Map.empty, None, None),
          source = Source.Http
        )
      )))


    val responder = system.actorOf(CliResponder.props(master, TestProbe().ref))

    val probe = TestProbe()
    probe.send(responder, RunningJobs)

    val msg = probe.receiveOne(1.second)
    val definitions = msg.asInstanceOf[List[JobDetails]]
    definitions.size shouldBe 1
  }

  it("should forward other messages to manager") {
    val master = mock(classOf[MasterService])
    val manager = TestProbe()
    val responder = system.actorOf(CliResponder.props(master, manager.ref))

    val probe = TestProbe()
    probe.send(responder, StopAllWorkers)

    manager.expectMsg(StopAllWorkers)
  }
}
