package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.Directives
import akka.stream.scaladsl.{Flow, Sink}
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

  val route = CorsDirective.cors() {
    path( "v2" / "api" / "jobs" / "ws") {
      handleWebsocketMessages(allEventsWsFlow())
    } ~
    path( "v2" / "api" / "jobs" / Segment / "ws") { jobId =>
      handleWebsocketMessages(jobWsFlow(jobId))
    }
  }

  private def jobWsFlow(id: String): Flow[Message, Message, Any]= {
    val source = streamer.eventsSource()
      .filter(_.id == id)
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
