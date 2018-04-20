package io.hydrosphere.mist.master.interfaces.http

import mist.api.data._
import akka.http.scaladsl.testkit.{ScalatestRouteTest, WSProbe}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.EventsStreamer
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.duration._

class WsApiSpec extends FunSpec
  with Matchers
  with ScalatestRouteTest {

  val mat = ActorMaterializer()
  implicit val keepAlive = 30 seconds

  it("should stream all events") {
    val testSource = Source[UpdateStatusEvent](List(
      StartedEvent("1", 1), StartedEvent("2", 1)
    ))

    val streamer = mock(classOf[EventsStreamer])
    when(streamer.eventsSource()).thenReturn(testSource)

    val route = new WSApi(streamer).route

    val client = WSProbe()

    WS("/v2/api/ws/all", client.flow) ~> route ~> check {
      isWebSocketUpgrade shouldBe true

      client.expectMessage("""{"id":"1","time":1,"event":"started"}""")
      client.expectMessage("""{"id":"2","time":1,"event":"started"}""")
    }
  }

  it("should stream for particular job") {

    val testSource = Source[SystemEvent](List(
      StartedEvent("1", 1),
      StartedEvent("2", 1),
      ReceivedLogs("1", Seq(LogEvent("1", "test", 1, 1, None)), 0),
      FinishedEvent("1", 1, JsMap("result" -> JsNumber(42)))
    ))

    val streamer = mock(classOf[EventsStreamer])
    when(streamer.eventsSource()).thenReturn(testSource)

    val route = new WSApi(streamer).route

    val client = WSProbe()

    WS("/v2/api/ws/jobs/1?withLogs=true", client.flow) ~> route ~> check {
      isWebSocketUpgrade shouldBe true

      client.expectMessage("""{"id":"1","time":1,"event":"started"}""")
      client.expectMessage("""{"id":"1","events":[{"from":"1","message":"test","timeStamp":1,"level":1}],"fileOffset":0,"event":"logs"}""")
      client.expectMessage("""{"id":"1","time":1,"result":{"result":42},"event":"finished"}""")
    }
  }

  it("should filter received logs events if flag wasn't set") {
    val testSource = Source[SystemEvent](List(
      StartedEvent("1", 1),
      StartedEvent("2", 1),
      ReceivedLogs("1", Seq(LogEvent("1", "test", 1, 1, None)), 0),
      FinishedEvent("1", 1, JsMap("result" -> JsNumber(42)))
    ))

    val streamer = mock(classOf[EventsStreamer])
    when(streamer.eventsSource()).thenReturn(testSource)

    val route = new WSApi(streamer).route

    val client = WSProbe()

    WS("/v2/api/ws/jobs/1", client.flow) ~> route ~> check {
      isWebSocketUpgrade shouldBe true

      client.expectMessage("""{"id":"1","time":1,"event":"started"}""")
      client.expectMessage("""{"id":"1","time":1,"result":{"result":42},"event":"finished"}""")
    }
  }
}
