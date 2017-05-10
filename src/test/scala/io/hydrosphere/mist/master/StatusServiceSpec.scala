package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.testkit.TestKit
import io.hydrosphere.mist.Messages.StatusMessages._
import io.hydrosphere.mist.jobs.JobDetails.{Status, Source}
import io.hydrosphere.mist.jobs.{Action, JobDetails, JobExecutionParams}
import io.hydrosphere.mist.master.store.JobRepository
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{FunSpecLike, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.concurrent.Future

class StatusServiceSpec extends TestKit(ActorSystem("testFront"))
  with FunSpecLike
  with Matchers
  with Eventually {

  val jobExecutionParams = JobExecutionParams(
    path = "path",
    className = "MyClass",
    namespace = "namespace",
    parameters = Map("1" -> 2),
    externalId = Some("externalId"),
    route = Some("route"),
    action = Action.Execute
  )

  it("should register jobs") {
    val store = mock(classOf[JobRepository])
    when(store.update(any[JobDetails]))
      .thenReturn(Future.successful(()))

    val status = system.actorOf(StatusService.props(store, Seq.empty))

    status ! Register("id", jobExecutionParams, Source.Async("Kafka"))

    eventually(timeout(Span(3, Seconds))) {
      verify(store).update(any[JobDetails])
    }
  }

  it("should update status") {
    val store = mock(classOf[JobRepository])
    when(store.get(any[String])).thenReturn({
      val jobDetails = JobDetails(
        configuration = jobExecutionParams,
        jobId = "id",
        source = Source.Http
      )
      Future.successful(Some(jobDetails))
    })

    val status = system.actorOf(StatusService.props(store, Seq.empty))

    status ! StartedEvent("id", System.currentTimeMillis())

    eventually(timeout(Span(1, Seconds))) {
      verify(store).update(any[JobDetails])
    }
  }


  describe("event conversion") {

    val conf = JobExecutionParams(
      "path",
      "className",
      "namespace",
      Map.empty, None, Some("sd"), Action.Execute)

    val baseDetails = JobDetails(
      configuration = conf,
      source = Source.Cli,
      jobId = "id"
    )

    val expected = Table(
      ("event", "details"),
      (QueuedEvent("id"), baseDetails.copy(status = Status.Queued)),
      (StartedEvent("id", 1), baseDetails.copy(status = Status.Running, startTime = Some(1))),
      (CanceledEvent("id", 1), baseDetails.copy(status = Status.Aborted, endTime = Some(1))),
      (FinishedEvent("id", 1, Map("1" -> 2)),
        baseDetails.copy(status = Status.Stopped, endTime = Some(1), jobResult = Some(Left(Map("1" -> 2))))),

      (FailedEvent("id", 1, "error"),
        baseDetails.copy(status = Status.Error, endTime = Some(1), jobResult = Some(Right("error"))))


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
