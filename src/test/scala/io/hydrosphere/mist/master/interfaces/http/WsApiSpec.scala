package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import io.hydrosphere.mist.Messages.StatusMessages.{FinishedEvent, StartedEvent, UpdateStatusEvent}
import io.hydrosphere.mist.master.EventsStreamer
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}

class WsApiSpec extends FunSpec
  with Matchers
  with ScalatestRouteTest {

  val mat = ActorMaterializer()

  it("should stream all events") {
    val testSource = Source[UpdateStatusEvent](List(
      StartedEvent("1", 1), StartedEvent("2", 1)
    ))

    val streamer = mock(classOf[EventsStreamer])
    when(streamer.eventsSource()).thenReturn(testSource)

    val route = new WSApi(streamer).route

    val client = WSProbe()

    WS("/v2/api/ws/all", client.flow) ~> route ~> check {
      isWebsocketUpgrade shouldBe true

      client.expectMessage("""{"id":"1","time":1,"event":"started"}""")
      client.expectMessage("""{"id":"2","time":1,"event":"started"}""")
    }
  }

  it("should stream for particular job") {

    val testSource = Source[UpdateStatusEvent](List(
      StartedEvent("1", 1),
      StartedEvent("2", 1),
      FinishedEvent("1", 1, Map("result" -> 42))
    ))

    val streamer = mock(classOf[EventsStreamer])
    when(streamer.eventsSource()).thenReturn(testSource)

    val route = new WSApi(streamer).route

    val client = WSProbe()

    WS("/v2/api/ws/jobs/1", client.flow) ~> route ~> check {
      isWebsocketUpgrade shouldBe true

      client.expectMessage("""{"id":"1","time":1,"event":"started"}""")
      client.expectMessage("""{"id":"1","time":1,"result":{"result":42},"event":"finished"}""")
    }
  }
}
