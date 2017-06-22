package io.hydrosphere.mist.master.logging

import java.nio.ByteOrder

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl.{Flow, Keep, Sink, Tcp}
import akka.util.{ByteString, Timeout}
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.hydrosphere.mist.Messages.StatusMessages.ReceivedLogs
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.master.{EventsStreamer, JobEventPublisher}

import scala.concurrent.Future
import scala.concurrent.duration._



class LogService(writer: LogsWriter) {

  val storeFlow: Flow[LogEvent, LogUpdate, Unit] = {
    Flow[LogEvent]
      .groupBy(1000, _.from)
      .groupedWithin(1000, 1 second)
      .mapAsync(10)(events => {
        val jobId = events.head.from
        writer.write(jobId, events)
      })
      .mergeSubstreams
  }

  def connectionFlow[B](
    eventHandler: Flow[LogEvent, B, Any],
    decodePoolSize: Int = 10): Flow[ByteString, ByteString, Unit] = {

    val kryoPool = {
      val inst = new ScalaKryoInstantiator
      inst.setRegistrationRequired(false)
      KryoPool.withByteArrayOutputStream(decodePoolSize, inst)
    }

    Flow[ByteString]
      .via(Framing.lengthField(4, 0, 1024 * 1024 * 8, ByteOrder.BIG_ENDIAN))
      .map(bs => {
        val bytes = bs.drop(4).toArray
        kryoPool.fromBytes(bytes, classOf[LogEvent])
      })
      .via(eventHandler)
      .map(_ => ByteString.empty)
      .filter(_ => false)
  }

}

object LogService {

  def start(
    host: String, port: Int,
    mappings: LogStorageMappings,
    eventPublishers: Seq[JobEventPublisher]
  )(implicit sys: ActorSystem, mat: ActorMaterializer): Future[Tcp.ServerBinding] = {

    val writer = LogsWriter(mappings, sys)
    val service = new LogService(writer)

    val eventsStreamerSink = Sink.foreach[LogUpdate](upd => {
      val event = ReceivedLogs(upd.jobId, upd.events, upd.bytesOffset)
      eventPublishers.foreach(_.notify(event))
    })

    Tcp().bind(host, port).toMat(Sink.foreach(conn => {

      val handle = service.connectionFlow(
        service.storeFlow.alsoTo(eventsStreamerSink)
      )
      conn.handleWith(handle)
    }))(Keep.left).run()
  }

}
