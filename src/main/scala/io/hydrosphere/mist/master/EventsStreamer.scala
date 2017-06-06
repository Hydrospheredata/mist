package io.hydrosphere.mist.master

import akka.actor._
import akka.stream.OverflowStrategy
import akka.stream.scaladsl._
import io.hydrosphere.mist.Messages.StatusMessages.UpdateStatusEvent

/**
  * TODO: Rewrite MQTT and Kafka publisher in akka-streams-way
  */
trait EventsStreamer {

  def eventsSource(): Source[UpdateStatusEvent, Unit]

  def push(event: UpdateStatusEvent): Unit

  def asPublisher: JobEventPublisher = {

    new JobEventPublisher {
      override def notify(event: UpdateStatusEvent): Unit =
        push(event)

      override def close(): Unit = {}
    }
  }
}

/**
  * Job events streaming source
  */
object EventsStreamer {

  private val BufferSize = 500

  def apply(system: ActorSystem): EventsStreamer = {
    val actor = system.actorOf(Props(new BroadcastSource))

    new EventsStreamer {
      override def eventsSource(): Source[UpdateStatusEvent, Unit] = {
        Source.actorRef[UpdateStatusEvent](BufferSize, OverflowStrategy.dropTail)
          .mapMaterializedValue(ref => actor.tell("subscribe", ref))
      }

      override def push(event: UpdateStatusEvent): Unit = actor ! event
    }

  }

  private class BroadcastSource extends Actor {

    var subscribers = Set.empty[ActorRef]

    override def receive: Receive = {
      case "subscribe" =>
        val ref = sender()
        subscribers += ref
        context.watch(ref)

      case event: UpdateStatusEvent =>
        subscribers.foreach(_ ! event)

      case Terminated(ref) =>
        subscribers -= ref
    }
  }

}
