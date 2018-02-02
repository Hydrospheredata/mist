package io.hydrosphere.mist.master.execution.status

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import io.hydrosphere.mist.master.Messages.StatusMessages.StartedEvent
import io.hydrosphere.mist.master.{JobDetails, TestData}
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Future

class JobStatusFlusherSpec extends TestKit(ActorSystem("job-status-flusher"))
  with FunSpecLike
  with Matchers
  with TestData {

  it("should flush status correctly") {
    val props = JobStatusFlusher.props(
      id = "id",
      get = (_) => Future.successful(mkDetails(JobDetails.Status.Initialized)),
      update = (_) => Future.successful(())
    )
    val flusher = TestActorRef(props)

    flusher ! StartedEvent("id", System.currentTimeMillis())
    fail("no implemented")
  }

}
