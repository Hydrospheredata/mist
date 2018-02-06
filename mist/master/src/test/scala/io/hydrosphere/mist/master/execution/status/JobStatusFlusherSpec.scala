package io.hydrosphere.mist.master.execution.status

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import io.hydrosphere.mist.master.JobDetails.Status
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.{JobDetails, TestData}
import mist.api.data._
import org.scalatest.{FunSpecLike, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._

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

  describe("event conversion") {

    val baseDetails = mkDetails(JobDetails.Status.Initialized)

    val expected = Table(
      ("event", "details"),
      (QueuedEvent("id"), baseDetails.copy(status = Status.Queued)),
      (StartedEvent("id", 1), baseDetails.copy(status = Status.Started, startTime = Some(1))),
      (CanceledEvent("id", 1), baseDetails.copy(status = Status.Canceled, endTime = Some(1))),
      (FinishedEvent("id", 1, JsLikeMap("1" -> JsLikeNumber(2))),
        baseDetails.copy(
          status = Status.Finished,
          endTime = Some(1),
          jobResult =
            Some(
              Right(JsLikeMap("1" -> JsLikeNumber(2)))
            )
        )),
      (FailedEvent("id", 1, "error"),
        baseDetails.copy(status = Status.Failed, endTime = Some(1), jobResult = Some(Left("error")))),
      (WorkerAssigned("id", "workerId"), baseDetails.copy(workerId = Some("workerId")))
    )

    it("should correct update job details") {
      forAll(expected) { (e: UpdateStatusEvent, d: JobDetails) =>
        JobStatusFlusher.applyStatusEvent(baseDetails, e) shouldBe d
      }
    }

    it("should ignore failure if job is canceled") {
      val canceled = baseDetails.copy(status = Status.Canceled)
      val event = FailedEvent("id", 1, "error")
      JobStatusFlusher.applyStatusEvent(canceled, event) shouldBe canceled
    }
  }
}
