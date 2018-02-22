package io.hydrosphere.mist.master.execution.status

import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.master.Messages.StatusMessages.{QueuedEvent, UpdateStatusEvent}
import io.hydrosphere.mist.master.logging.{JobLogger, LogService}
import io.hydrosphere.mist.master.store.JobRepository
import io.hydrosphere.mist.master.{ActorSpec, EventsStreamer, JobDetails, TestData}
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

class StatusReporterSpec extends ActorSpec("status-reporter") with TestData with Eventually with MockitoSugar {

  it("should flush and stream") {
    val repo = mock[JobRepository]
    when(repo.get(any[String])).thenSuccess(Some(mkDetails(JobDetails.Status.Initialized)))
    when(repo.update(any[JobDetails])).thenSuccess(())
    val logService = mock[LogService]
    when(logService.getJobLogger(any[String]))
      .thenReturn(JobLogger.NOOP)

    val streamer = mock[EventsStreamer]
    val reporter = StatusReporter.reporter(repo, streamer, logService)

    reporter.reportPlain(QueuedEvent("id"))

    verify(streamer).push(any[UpdateStatusEvent])

    eventually(timeout(Span(3, Seconds))) {
      verify(repo).get(any[String])
      verify(repo).update(any[JobDetails])
    }
  }

}
