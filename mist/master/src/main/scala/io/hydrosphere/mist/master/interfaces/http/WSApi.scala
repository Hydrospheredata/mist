package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives
import akka.stream.scaladsl.{Flow, Sink}
import io.hydrosphere.mist.master.EventsStreamer
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import spray.json._

import scala.language.postfixOps

/**
  * Router for requests via websocket
  */
class WSApi(streamer: EventsStreamer) {

  import Directives._
  import JsonCodecs._

  val route = {
    pathPrefix("v2" / "api"/ "ws" ) { parameter('withLogs ? true)  { withLogs =>
      path("all") {
        get {
          handleWebSocketMessages(allEventsWsFlow(withLogs))
        }
      } ~
      path("jobs" / Segment) { jobId =>
        get {
          handleWebSocketMessages(jobWsFlow(jobId, withLogs))
        }
      }
    }}
  }

  private def jobWsFlow(id: String, withLogs: Boolean): Flow[Message, Message, Any] = {
    val source = streamer.eventsSource()
      .filter({
        case e: UpdateStatusEvent => e.id == id
        case e: ReceivedLogs if withLogs => e.id == id
        case _ => false
      })
      .map(toWsMessage)

    val sink = Sink.ignore
    Flow.fromSinkAndSource(sink, source)
  }

  private def allEventsWsFlow(withLogs: Boolean): Flow[Message, Message, Any] = {
    val source = streamer.eventsSource()
      .filter({
        case _: ReceivedLogs => withLogs
        case _ => true
      })
      .map(toWsMessage)

    val sink = Sink.ignore
    Flow.fromSinkAndSource(sink, source)
  }

  private def toWsMessage(e: SystemEvent): Message = TextMessage.Strict(e.toJson.toString())
}
