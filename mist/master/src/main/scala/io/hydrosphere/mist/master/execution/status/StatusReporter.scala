package io.hydrosphere.mist.master.execution.status

import akka.actor.ActorSystem
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.{EventsStreamer, JobDetails}
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.logging.LogService
import io.hydrosphere.mist.master.store.JobRepository

import scala.concurrent.Future

trait StatusReporter {

  def report(ev: ReportedEvent): Unit
  def reportPlain(e: UpdateStatusEvent): Unit = report(ReportedEvent.plain(e))
  def reportWithFlushCallback(e: UpdateStatusEvent): Future[JobDetails] = {
    val ev = ReportedEvent.withCallback(e)
    report(ev)
    ev.callback.future
  }
}

object StatusReporter {

  val NOOP = new StatusReporter {
    override def report(ev: ReportedEvent): Unit = ()
  }

  /**
    * Send status updates to store + async interfaces
    */
  def reporter(
    repo: JobRepository,
    streamer: EventsStreamer,
    logService: LogService
  )(implicit sys: ActorSystem): StatusReporter = {
    val flusher = sys.actorOf(StoreFlusher.props(repo, logService))
    new StatusReporter {
      override def report(ev: ReportedEvent): Unit = {
        flusher ! ev
        streamer.push(ev.e)
      }
    }
  }
}
