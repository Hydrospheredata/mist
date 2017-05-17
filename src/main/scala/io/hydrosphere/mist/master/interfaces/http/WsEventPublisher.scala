package io.hydrosphere.mist.master.interfaces.http

import java.util.UUID

import akka.actor._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import io.hydrosphere.mist.Messages.StatusMessages.UpdateStatusEvent


trait JobEventsStreamer {

  def eventsFlow(): Flow[String, UpdateStatusEvent, Any]

  def putEvent(event: UpdateStatusEvent): Unit
}

object JobEventsStreamer {

  def apply(system: ActorSystem): JobEventsStreamer = {

    val actor = system.actorOf(Props(new Actor with ActorLogging {

      var subscribers = Set.empty[ActorRef]

      override def receive: Receive = {
        case "on" =>
          subscribers += sender()
          log.info("new subscriber")
        case "off" =>
          subscribers -= sender()
          log.info("leave!")

        case event: UpdateStatusEvent =>
          subscribers.foreach(_ ! event)
      }

    }))

    new JobEventsStreamer {
      override def eventsFlow(): Flow[String, UpdateStatusEvent, Any] = {
        val id = UUID.randomUUID().toString
        val in = Flow[String].to(Sink.actorRef(actor, "off"))

        val out = Source.actorRef[UpdateStatusEvent](1, OverflowStrategy.fail)
          .mapMaterializedValue(ref => actor.tell("on", ref))

        Flow.fromSinkAndSource(in, out)
      }

      override def putEvent(event: UpdateStatusEvent): Unit = actor ! event
    }

  }

}
