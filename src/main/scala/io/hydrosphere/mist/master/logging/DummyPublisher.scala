package io.hydrosphere.mist.master.logging

import akka.actor.Props
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.Cancel
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent

/**
  * Use actor publisher without any logic to merge two ways of producing logs:
  * - via incoming tcp requests
  * - local calls
  */
class DummyPublisher extends ActorPublisher[LogEvent] {
  override def receive: Receive = {
    case e: LogEvent =>
      println(s"EVENT $e")
      onNext(e)
//    case Cancel => context.stop(self)
  }
}

object DummyPublisher {
  def props():Props = Props(classOf[DummyPublisher])
}

