package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.ws._
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.Supervision.resumingDecider
import akka.stream.{ActorAttributes, Supervision}
import akka.stream.scaladsl.{Flow, Sink}
import io.hydrosphere.mist.master.EventsStreamer
import io.hydrosphere.mist.master.Messages.StatusMessages._
import io.hydrosphere.mist.master.interfaces.JsonCodecs

import scala.concurrent.duration._
import spray.json._

import scala.language.postfixOps

/**
  * Router for requests via websocket
  */
class WSApi(streamer: EventsStreamer)(implicit val keepAliveTimeout: FiniteDuration) {

  import Directives._
  import JsonCodecs._

  val route: Route = {
    pathPrefix("v2" / "api"/ "ws" ) { parameter('withLogs ? false)  { withLogs =>
      path("all") {
        get {
          handleWebSocketMessagesWithKeepAlive(allEventsWsFlow(withLogs))
        }
      } ~
      path("jobs" / Segment) { jobId =>
        get {
          handleWebSocketMessagesWithKeepAlive(jobWsFlow(jobId, withLogs))
        }
      }
    }}
  }

  private def handleWebSocketMessagesWithKeepAlive(handler: Flow[Message, Message, akka.NotUsed]): Route =
    handleWebSocketMessages(handler
      .withAttributes(supervisionStrategy(resumingDecider))
      .keepAlive(
        keepAliveTimeout,
        () => TextMessage.Strict(KeepAliveEvent.asInstanceOf[SystemEvent].toJson.toString())
      ))


  private def jobWsFlow(id: String, withLogs: Boolean): Flow[Message, Message, akka.NotUsed] = {
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

  private def allEventsWsFlow(withLogs: Boolean): Flow[Message, Message, akka.NotUsed] = {
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
