package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives
import akka.stream.scaladsl.{Flow, Sink}
import io.hydrosphere.mist.Messages.StatusMessages.{ReceivedLogs, UpdateStatusEvent}
import io.hydrosphere.mist.master.EventsStreamer
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
    path( "v2" / "api" / "ws") {
      get {
        handleWebsocketMessages(allEventsWsFlow())
      }
    } ~
    path( "v2" / "api" / "jobs" / Segment / "ws") { jobId =>
      get {
        handleWebsocketMessages(jobWsFlow(jobId))
      }
    }
  }

  private def jobWsFlow(id: String): Flow[Message, Message, Any]= {
    val source = streamer.eventsSource()
      .filter({
        case e: UpdateStatusEvent => e.id == id
        case e: ReceivedLogs => e.jobId == id
        case _ => false
      })
      .map(e => {
        val json = e.toJson.toString()
        TextMessage.Strict(json)
      })

    val sink = Sink.ignore
    Flow.fromSinkAndSource(sink, source)
  }

  private def allEventsWsFlow(): Flow[Message, Message, Any] = {
    val source = streamer.eventsSource().map(e => {
      val json = e.toJson.toString()
      TextMessage.Strict(json)
    })

    val sink = Sink.ignore
    Flow.fromSinkAndSource(sink, source)
  }
}
