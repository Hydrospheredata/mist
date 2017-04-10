package io.hydrosphere.mist.master

import java.io.File
import java.util.UUID

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.store.InMemoryJobRepository
import io.hydrosphere.mist.master.JobQueue.{DequeueJob, EnqueueJob}
import org.scalatest.{FunSpecLike, Matchers}

class JobDispatcherTest extends TestKit(ActorSystem ("mist-tests") ) with ImplicitSender with FunSpecLike with Matchers {

  private def job() = {
    val config = JobConfiguration.fromConfig(ConfigFactory.parseFile(new File(MistConfig.Http.routerConfigPath)).resolve().getConfig("simple-context"))
    val definition = JobDefinition("simple-context", config.get)
    val jobConfiguration = JobExecutionParams.fromDefinition(definition, Action.Execute, Map.empty[String, Any], None)
    JobDetails(jobConfiguration, JobDetails.Source.Cli, UUID.randomUUID().toString)
  }


  describe("Job Dispatcher") {
    
    val store = new InMemoryJobRepository()
    val probe = TestProbe()
    val actorRef = TestActorRef(Props(classOf[JobDispatcher], probe.ref, store))
    val jobDetails = job()
    
    it("should enqueue job") {
      actorRef ! jobDetails
      probe.expectMsg(EnqueueJob(jobDetails))
      store.get(jobDetails.jobId) shouldBe Some(jobDetails)
    }
    
    it("should update job in store after changes") {
      actorRef ! jobDetails.withStatus(JobDetails.Status.Running)
      store.get(jobDetails.jobId).map(_.status).get shouldBe JobDetails.Status.Running
    }
    
    it("should dequeue job after getting results") {
      actorRef ! jobDetails.withStatus(JobDetails.Status.Stopped)
      probe.expectMsg(DequeueJob(jobDetails))
      store.get(jobDetails.jobId).map(_.status).get shouldBe JobDetails.Status.Stopped
    }
    
  }
  
}
