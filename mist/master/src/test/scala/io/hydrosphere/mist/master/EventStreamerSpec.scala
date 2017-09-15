package io.hydrosphere.mist.master

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import io.hydrosphere.mist.master.Messages.StatusMessages.StartedEvent
import org.scalatest.{FunSpecLike, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class EventStreamerSpec extends TestKit(ActorSystem("streamer"))
  with FunSpecLike
  with Matchers {

  implicit val materializer = ActorMaterializer()

  it("should broadcast events") {
    val streamer = EventsStreamer(system)

    val f = streamer.eventsSource()
      .take(2)
      .runWith(Sink.seq)

    streamer.push(StartedEvent("1", 1))
    streamer.push(StartedEvent("2", 1))
    streamer.push(StartedEvent("3", 1))

    val events = Await.result(f, Duration.Inf)
    events should contain allOf (
      StartedEvent("1", 1),
      StartedEvent("2", 1)
    )
  }

}
