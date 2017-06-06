package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages.JobMessages.{JobParams, RunJobRequest}
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobDetails.{Source, Status}
import io.hydrosphere.mist.jobs.{Action, JobDetails}
import io.hydrosphere.mist.master.store.JobRepository
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Future

class StatusServiceSpec extends TestKit(ActorSystem("testFront"))
  with FunSpecLike
  with Matchers
  with Eventually {

  val params = JobParams("path", "className", Map.empty, Action.Execute)

  it("should register jobs") {
    val store = mock(classOf[JobRepository])
    when(store.update(any[JobDetails]))
      .thenReturn(Future.successful(()))

    val status = system.actorOf(StatusService.props(store, Seq.empty))

    status ! Register(
      RunJobRequest("id", params),
      "endpoint",
      "context",
       Source.Async("Kafka"),
       None
    )

    eventually(timeout(Span(3, Seconds))) {
      verify(store).update(any[JobDetails])
    }
  }

  it("should update status in storage and call publisher") {
    val store = mock(classOf[JobRepository])
    when(store.get(any[String])).thenReturn({
      val jobDetails = JobDetails(
        params = params,
        jobId = "id",
        source = Source.Http,
        endpoint = "endpoint",
        context = "context",
        externalId = None
      )
      Future.successful(Some(jobDetails))
    })
    when(store.update(any[JobDetails])).thenReturn(Future.successful(()))

    val publisher = mock(classOf[JobEventPublisher])

    val status = system.actorOf(StatusService.props(store, Seq(publisher)))

    status ! StartedEvent("id", System.currentTimeMillis())

    eventually(timeout(Span(1, Seconds))) {
      verify(store).update(any[JobDetails])
    }

    eventually(timeout(Span(1, Seconds))) {
      verify(publisher).notify(any[StartedEvent])
    }
  }

  describe("event conversion") {

    val baseDetails = JobDetails(
        params = params,
        jobId = "id",
        source = Source.Http,
        endpoint = "endpoint",
        context = "context",
        externalId = None
      )

    val expected = Table(
      ("event", "details"),
      (QueuedEvent("id", "yoyo"), baseDetails.copy(status = Status.Queued, workerId = Some("yoyo"))),
      (StartedEvent("id", 1), baseDetails.copy(status = Status.Running, startTime = Some(1))),
      (CanceledEvent("id", 1), baseDetails.copy(status = Status.Aborted, endTime = Some(1))),
      (FinishedEvent("id", 1, Map("1" -> 2)),
        baseDetails.copy(status = Status.Stopped, endTime = Some(1), jobResult = Some(Right(Map("1" -> 2))))),

      (FailedEvent("id", 1, "error"),
        baseDetails.copy(status = Status.Error, endTime = Some(1), jobResult = Some(Left("error"))))


    )

    it("should correct update job details") {
      forAll(expected) { (e: UpdateStatusEvent, d: JobDetails) =>
        StatusService.applyStatusEvent(baseDetails, e) shouldBe d
      }
    }

    it("should ignore failure if job is canceled") {
      val canceled = baseDetails.copy(status = Status.Aborted)
      val event = FailedEvent("id", 1, "error")
      StatusService.applyStatusEvent(canceled, event) shouldBe canceled
    }
  }

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))
}
