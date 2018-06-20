package io.hydrosphere.mist.master.logging

import java.nio.ByteOrder

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl._
import akka.stream.{ActorAttributes, ActorMaterializer, OverflowStrategy}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.Messages.StatusMessages.ReceivedLogs
import io.hydrosphere.mist.master.{EventsStreamer, LogStoragePaths}
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

trait LogStreams extends Logger {

  import ActorAttributes.supervisionStrategy
  import akka.stream.Supervision.resumingDecider

  /**
    * Storing batched logEvent via writer and produce Event
    *   for async interfaces
    */
  def storeFlow(writer: LogsWriter): Flow[LogEvent, LogUpdate, NotUsed] = {
    def fTry[A](f: Future[A]): Future[Try[A]] = {
      f.map(a => Success(a)).recover({case e => Failure(e)})
    }

    def batchWrite(elems: Seq[LogEvent]): Future[List[LogUpdate]] = {
      val futures = elems.groupBy(_.from).map({case (id, perJob) => fTry(writer.write(id, perJob))})
      Future.sequence(futures).map(all => {
        all.foldLeft(List.empty[LogUpdate]) {
          case (acc, Success(upd)) => upd :: acc
          case (acc, Failure(e)) =>
            logger.error("Batch writing error", e)
            acc
        }
      })
    }

    Flow[LogEvent]
      .groupedWithin(LogStreams.BatchSize, LogStreams.BatchWindowTime)
      .mapAsync(1)(batchWrite)
      .withAttributes(supervisionStrategy(resumingDecider))
      .mapConcat(identity)
  }

  /**
    * Tcp connection handler with kryo deserializer
    */
  def tcpConnectionFlow[B](
    eventHandler: Flow[LogEvent, B, Any],
    decodePoolSize: Int = 10): Flow[ByteString, ByteString, NotUsed] = {

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

  def eventStreamerSink(streamer: EventsStreamer): Sink[LogUpdate, Future[Done]] = {
    Sink.foreach[LogUpdate](upd => {
      val event = ReceivedLogs(upd.jobId, upd.events, upd.bytesOffset)
      streamer.push(event)
    })
  }

  /**
    * Returns actor ref that consumes incoming logEvents
    */
  def runStoreFlow(
    writer: LogsWriter,
    streamer: EventsStreamer
  )(implicit mat: ActorMaterializer): ActorRef = {
    val source = Source.actorRef[LogEvent](LogStreams.LogEventBufferSize, OverflowStrategy.dropHead)

    val sink = eventStreamerSink(streamer)

    val (ref, f) = source.via(storeFlow(writer))
      .toMat(sink)(Keep.both).run()

    f.onComplete({
      case Success(_) => logger.info("Log storing flow was completed")
      case Failure(e) => logger.error("Log storing flow was completed with failure", e)
    })

    ref
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

trait JobLoggersFactory {
  def getJobLogger(id: String): JobLogger
}

/**
  * Wrapper around local input and tcp server binding
  */
class LogService(
  storeInput: ActorRef,
  serverBinding: Tcp.ServerBinding
) extends JobLoggersFactory {
  override def getJobLogger(id: String): JobLogger = JobLogger.fromActorRef(id, storeInput)
  def close(): Future[Unit] = serverBinding.unbind()

}

object LogStreams extends LogStreams {

  val LogEventBufferSize = 10000
  val BatchSize = 1000
  val BatchWindowTime = 1 second

  def runService(
    host: String,
    port: Int,
    paths: LogStoragePaths,
    eventsStreamer: EventsStreamer
  )(implicit sys: ActorSystem, mat: ActorMaterializer): Future[LogService] = {

    val writer = LogsWriter(paths, sys)
    val storeInput = runStoreFlow(writer, eventsStreamer)
    val f = runTcpServer(host, port, storeInput)
    f.map(binding => new LogService(storeInput, binding))(sys.dispatcher)
  }
}

