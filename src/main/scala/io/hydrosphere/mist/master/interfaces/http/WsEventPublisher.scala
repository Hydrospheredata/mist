package io.hydrosphere.mist.master.interfaces.http

import akka.actor._
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl._
import io.hydrosphere.mist.Messages.StatusMessages.UpdateStatusEvent
import io.hydrosphere.mist.master.JobEventPublisher
import io.hydrosphere.mist.utils.Logger
import org.reactivestreams.Publisher


class WsEventPublisher(
  ref: ActorRef,
  publisher: Publisher[UpdateStatusEvent]
) extends JobEventPublisher {

  override def notify(event: UpdateStatusEvent): Unit =
    ref ! event

  override def close(): Unit = ref ! PoisonPill

  def newFlow(id: String) = {
    val s = Source.fromPublisher(publisher).filter(_.id == id)
    Flow.fromSinkAndSource(Sink.)
  }
}


object WsEventPublisher extends Logger {

  def blabla(mat: ActorMaterializer): WsEventPublisher = {
    implicit val materializer = mat
    val producer = Source.actorRef[UpdateStatusEvent](100, OverflowStrategy.fail)
    val (ref, publisher) = producer.toMat(Sink.asPublisher(true))(Keep.both).run()
    new WsEventPublisher(ref)
  }

}

