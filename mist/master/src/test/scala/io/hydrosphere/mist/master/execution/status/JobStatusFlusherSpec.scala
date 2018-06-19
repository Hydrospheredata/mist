package io.hydrosphere.mist.master.execution.status

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import io.hydrosphere.mist.master.JobDetails.Status
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.{ActorSpec, JobDetails, TestData}
import io.hydrosphere.mist.master.logging.JobLogger
import mist.api.data._
import org.scalatest.concurrent.Eventually
import org.scalatest.{FunSpecLike, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{Future, Promise}

class JobStatusFlusherSpec extends ActorSpec("job-status-flusher") with TestData with Eventually {

  it("should flush status correctly") {
    val initial = Promise[JobDetails]

    val updateResult = new AtomicReference[Option[JobDetails]](None)
    val props = JobStatusFlusher.props(
      id = "id",
      get = (_) => initial.future,
      update = (d: JobDetails) => {updateResult.set(Some(d));Future.successful(())},
      loggerF = _ => JobLogger.NOOP
    )
    val flusher = TestActorRef(props)

    flusher ! ReportedEvent.plain(QueuedEvent("id"))
    flusher ! ReportedEvent.plain(StartedEvent("id", System.currentTimeMillis()))
    flusher ! ReportedEvent.plain(FinishedEvent("id", System.currentTimeMillis(), JsNumber(42)))

    initial.success(mkDetails(JobDetails.Status.Initialized))

    eventually(timeout(Span(3, Seconds))) {
      val value = updateResult.get
      value.isDefined shouldBe true

      val d = value.get
      d.status shouldBe JobDetails.Status.Finished
    }
  }

  describe("event conversion") {

    val baseDetails = mkDetails(JobDetails.Status.Initialized)

    val expected = Table(
      ("event", "details"),
      (QueuedEvent("id"), baseDetails.copy(status = Status.Queued)),
      (StartedEvent("id", 1), baseDetails.copy(status = Status.Started, startTime = Some(1))),
      (CancellingEvent("id", 1), baseDetails.copy(status = Status.Cancelling)),
      (CancelledEvent("id", 1), baseDetails.copy(status = Status.Canceled, endTime = Some(1))),
      (FinishedEvent("id", 1, JsMap("1" -> JsNumber(2))),
        baseDetails.copy(
          status = Status.Finished,
          endTime = Some(1),
          jobResult =
            Some(
              Right(JsMap("1" -> JsNumber(2)))
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
  }
}
