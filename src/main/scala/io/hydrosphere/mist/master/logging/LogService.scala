package io.hydrosphere.mist.master.logging

import java.nio.ByteOrder
import java.nio.file.{Files, StandardOpenOption}

import akka.stream.io.Framing
import akka.stream.scaladsl.{FileIO, Flow}
import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import io.hydrosphere.mist.api.logging.MistLogging.LogEvent

case class LogUpdate(
  jobId: String,
  events: Seq[LogEvent],
  bytesOffset: Long
)


class LogService(storage: LogsStore) {

  def writeToLog(entryId: String, events: Seq[LogEvent]): Unit = {
    val path = storage.mkPath(entryId)
    val data = events.map(_.mkString).mkString("\n")

    val f = path.toFile
    if (!f.exists())
      Files.createFile(path)

    Files.write(path, data.getBytes, StandardOpenOption.APPEND)

    FileIO.toFile(path.toFile, append = true)
  }

  def eventsToByteString(events: Seq[LogEvent]): ByteString = {
    val data = events.map(_.mkString).mkString("\n")
    ByteString(data)
  }

//  def storeFlow: Flow[LogEvent, LogEvent, Unit] = {
//    val x = Flow[LogEvent]
//      .groupBy(1000, _.from)
//      .groupedWithin(1000, 1 second)
//      .map(e => e.head.from -> eventsToByteString(e))
//      .mapAsync()
//  }
//
//  def writeToFile(entryId: String, events: Seq[LogEvent]): Future[LogUpdate] = {
//    Future {
//
//    }
//  }

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

//  def start(config: LogServiceConfig): LogService = {
//    import config._
//
//    val store = LogsStore(config.dumpDirectory)
//
//    val handler = Flow[LogEvent]
//      .groupBy(1000, _.from)
//      .groupedWithin(100, 1 second)
//      .mapAsync()
//
//    val tcp = Tcp().bind(host, port)
//    tcp.toMat(Sink.foreach(connection =>
//
//      val flow = connectionFlow()
//      connection.handleWith()
//    ))
//  }


}
