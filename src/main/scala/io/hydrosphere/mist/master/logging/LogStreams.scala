package io.hydrosphere.mist.master.logging

import java.nio.ByteOrder

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.hydrosphere.mist.Messages.StatusMessages.ReceivedLogs
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent
import io.hydrosphere.mist.master.JobEventPublisher

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

trait LogStreams {

  /**
    * Storing batched logEvent via writer and produce Event
    *   for async interfaces
    */
  def storeFlow(writer: LogsWriter): Flow[LogEvent, LogUpdate, Unit] = {
    Flow[LogEvent]
      .groupBy(1000, _.from)
      .groupedWithin(1000, 1 second)
      .mapAsync(10)(events => {
        val jobId = events.head.from
        writer.write(jobId, events)
      })
      .mergeSubstreams
  }

  /**
    * Tcp connection handler with kryo deserializer
    */
  def tcpConnectionFlow[B](
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

  def sinkToPublishers(seq: Seq[JobEventPublisher]): Sink[LogUpdate, Any] = {
    Sink.foreach[LogUpdate](upd => {
      val event = ReceivedLogs(upd.jobId, upd.events, upd.bytesOffset)
      seq.foreach(_.notify(event))
    })
  }

  /**
    * Returns actor ref that consumes incoming logEvents
    */
  def runStoreFlow(
    writer: LogsWriter,
    publishers: Seq[JobEventPublisher]
  )(implicit mat: ActorMaterializer): ActorRef = {
    val source = Source.actorRef[LogEvent](10000, OverflowStrategy.dropHead)
    val sink = sinkToPublishers(publishers)

    source.via(storeFlow(writer)).toMat(sink)(Keep.left).run()
  }

  /**
    *
    * @param consumerRef - ref to StoreFlow consumer (see runStoreFlow)
    */
  def runTcpServer(host: String, port: Int, consumerRef: ActorRef)
    (implicit sys: ActorSystem, mat: ActorMaterializer): Future[Tcp.ServerBinding] = {

    val handle = tcpConnectionFlow(
      Flow[LogEvent].map(e => {consumerRef ! e; ()})
    )
    Tcp().bind(host, port).toMat(Sink.foreach(conn => {
      conn.handleWith(handle)
    }))(Keep.left).run()
  }

}

/**
  * Wrapper around local input and tcp server binding
  */
class LogService(
  storeInput: ActorRef,
  serverBinding: Tcp.ServerBinding
) {

  def getLogger: JobsLogger = JobsLogger.fromActorRef(storeInput)

  def close(): Future[Unit] = serverBinding.unbind()

}

object LogStreams extends LogStreams {

  def runService(
    host: String,
    port: Int,
    mappings: LogStorageMappings,
    publishers: Seq[JobEventPublisher]
  )(implicit sys: ActorSystem, mat: ActorMaterializer): LogService = {

    val writer = LogsWriter(mappings, sys)

    val storeInput = runStoreFlow(writer, publishers)
    val f = runTcpServer(host, port, storeInput)
    val binding = Await.result(f, Duration.Inf)

    new LogService(storeInput, binding)
  }
}

