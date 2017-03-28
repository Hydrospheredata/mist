package io.hydrosphere.mist.master

import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import io.hydrosphere.mist.jobs.{FullJobConfigurationBuilder, JobDetails}
import io.hydrosphere.mist.jobs.store.InMemoryJobRepository
import io.hydrosphere.mist.master.JobQueue.{DequeueJob, EnqueueJob}
import org.scalatest.{FunSpecLike, Matchers}

class JobDispatcherTest extends TestKit(ActorSystem ("mist-tests") ) with ImplicitSender with FunSpecLike with Matchers {

  describe("Job Dispatcher") {
    
    val store = new InMemoryJobRepository()
    val probe = TestProbe()
    val actorRef = TestActorRef(Props(classOf[JobDispatcher], probe.ref, store))
    val job = JobDetails(
      FullJobConfigurationBuilder().fromRouter("simple-context", Map.empty[String, Any], None).build(), 
      JobDetails.Source.Cli, 
      UUID.randomUUID().toString
    )
    
    it("should enqueue job") {
      actorRef ! job
      probe.expectMsg(EnqueueJob(job))
      store.get(job.jobId) shouldBe Some(job)
    }
    
    it("should update job in store after changes") {
      actorRef ! job.withStatus(JobDetails.Status.Running)
      store.get(job.jobId).map(_.status).get shouldBe JobDetails.Status.Running
    }
    
    it("should dequeue job after getting results") {
      actorRef ! job.withStatus(JobDetails.Status.Stopped)
      probe.expectMsg(DequeueJob(job))
      store.get(job.jobId).map(_.status).get shouldBe JobDetails.Status.Stopped
    }
    
  }
  
}
