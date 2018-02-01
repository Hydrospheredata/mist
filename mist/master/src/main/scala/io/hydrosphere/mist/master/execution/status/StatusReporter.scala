package io.hydrosphere.mist.master.execution.status

import akka.actor.ActorSystem
import io.hydrosphere.mist.master.EventsStreamer
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.store.JobRepository

trait StatusReporter {

  def report(ev: UpdateStatusEvent): Unit

}

object StatusReporter {

  /**
    * Send status updates to store + async interfaces
    */
  def reporter(repo: JobRepository, streamer: EventsStreamer)(implicit sys: ActorSystem): StatusReporter = {
    val flusher = sys.actorOf(StoreFlusher.props(repo))
    new StatusReporter {
      override def report(ev: UpdateStatusEvent): Unit = {
        flusher ! ev
        streamer.push(ev)
      }
    }
  }
}
