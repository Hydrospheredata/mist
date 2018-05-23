package io.hydrosphere.mist.master.execution.status

import java.util.concurrent.atomic.AtomicReference

import akka.testkit.TestActorRef
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.logging.JobLogger
import io.hydrosphere.mist.master.{ActorSpec, JobDetails, TestData}
import mist.api.data.JsNumber
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.{Future, Promise}

class StoreFlusherSpec extends ActorSpec("store-flusher") with TestData with Eventually {

  it("should flush job statuses") {
    val initial1 = Promise[JobDetails]
    val initial2 = Promise[JobDetails]

    val updateResult1 = new AtomicReference[Option[JobDetails]](None)
    val updateResult2 = new AtomicReference[Option[JobDetails]](None)
    val props = StoreFlusher.props(
      get = (id: String) => id match {
        case "1" => initial1.future
        case "2" => initial2.future
      },
      update = (d: JobDetails) => {
        d.jobId match {
          case "1" => updateResult1.set(Some(d))
          case "2" => updateResult2.set(Some(d))
        }
        Future.successful(())
      },
      jobLoggerF = _ => JobLogger.NOOP
    )
    val flusher = TestActorRef(props)

    Seq("1", "2").foreach(id => {
      flusher ! ReportedEvent.plain(QueuedEvent(id))
      flusher ! ReportedEvent.plain(StartedEvent(id, System.currentTimeMillis()))
      flusher ! ReportedEvent.plain(FinishedEvent(id, System.currentTimeMillis(), JsNumber(42)))
    })
    initial1.success(mkDetails(JobDetails.Status.Initialized).copy(jobId = "1"))
    initial2.success(mkDetails(JobDetails.Status.Initialized).copy(jobId = "2"))

    def test(ref: AtomicReference[Option[JobDetails]]): Unit = {
      val value = ref.get
      value.isDefined shouldBe true

      val d = value.get
      d.status shouldBe JobDetails.Status.Finished
    }

    eventually(timeout(Span(3, Seconds))) {
      test(updateResult1)
      test(updateResult2)
    }
  }
}
