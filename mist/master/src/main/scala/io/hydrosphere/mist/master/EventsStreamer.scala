package io.hydrosphere.mist.master

import akka.actor._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import io.hydrosphere.mist.master.Messages.StatusMessages.{UpdateStatusEvent, SystemEvent}

trait EventsStreamer {

  def eventsSource(): Source[SystemEvent, akka.NotUsed]

  def push(event: SystemEvent): Unit

}

/**
  * Job events streaming source
  */
object EventsStreamer {

  private val BufferSize = 500

  def apply(system: ActorSystem): EventsStreamer = {
    val actor = system.actorOf(Props(new BroadcastSource))

    new EventsStreamer {
      override def eventsSource(): Source[SystemEvent, akka.NotUsed] = {
        Source.actorRef[UpdateStatusEvent](BufferSize, OverflowStrategy.dropTail)
          .mapMaterializedValue(ref => {actor.tell("subscribe", ref); akka.NotUsed})
      }

      override def push(event: SystemEvent): Unit = actor ! event
    }

  }

  private class BroadcastSource extends Actor {

    var subscribers = Set.empty[ActorRef]

    override def receive: Receive = {
      case "subscribe" =>
        val ref = sender()
        subscribers += ref
        context.watch(ref)

      case event: SystemEvent =>
        subscribers.foreach(_ ! event)

      case Terminated(ref) =>
        subscribers -= ref
    }
  }

}
