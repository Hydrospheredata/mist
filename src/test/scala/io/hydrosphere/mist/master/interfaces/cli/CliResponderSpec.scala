package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.Messages.ListRoutes
import io.hydrosphere.mist.Messages.StatusMessages.RunningJobs
import io.hydrosphere.mist.Messages.WorkerMessages.StopAllWorkers
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs.jar.JobsLoader
import io.hydrosphere.mist.jobs.{Action, JobDetails, JvmJobInfo, PyJobInfo}
import io.hydrosphere.mist.master.{JobService, MasterService}
import io.hydrosphere.mist.master.models.{EndpointConfig, FullEndpointInfo}
import org.mockito.Mockito._
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class CliResponderSpec extends TestKit(ActorSystem("cliResponderTest"))
  with FunSpecLike
  with Matchers {

  it("should return routes list") {
    val epConfig = EndpointConfig("name", "path", "className", "context")
    val scalaJobClass = io.hydrosphere.mist.jobs.jar.MultiplyJob.getClass
    val infos = Seq(
      PyJobInfo,
      JvmJobInfo(JobsLoader.Common.loadJobClass(scalaJobClass.getName).get)
    ).map(i => FullEndpointInfo(epConfig, i))

    val master = mock(classOf[MasterService])
    when(master.endpointsInfo).thenReturn(Future.successful(infos))

    val responder = system.actorOf(CliResponder.props(master, TestProbe().ref))

    val probe = TestProbe()
    probe.send(responder, ListRoutes)

    val msg = probe.receiveOne(1.second)
    val definitions = msg.asInstanceOf[Seq[FullEndpointInfo]]
    definitions.size shouldBe 2
  }

  it("should return running jobs") {
    val master = mock(classOf[MasterService])
    val jobService = mock(classOf[JobService])
    when(jobService.activeJobs())
      .thenReturn(Future.successful(List(
        JobDetails(
          params = JobParams("path", "className", Map.empty, Action.Execute),
          jobId = "id",
          source = Source.Http,
          endpoint = "endpoint",
          context = "context",
          externalId = None,
          workerId = "workerId"
        )
    )))

    when(master.jobService).thenReturn(jobService)

    val responder = system.actorOf(CliResponder.props(master, TestProbe().ref))

    val probe = TestProbe()
    probe.send(responder, RunningJobs)

    val msg = probe.receiveOne(1.second)
    val definitions = msg.asInstanceOf[Seq[JobDetails]]
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
