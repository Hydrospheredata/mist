package io.hydrosphere.mist.worker.logging

import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Keep, Sink, Source, Tcp}
import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.core.logging.{Level, LogEvent}
import io.hydrosphere.mist.worker.logging.RemoteLogsWriter.Key
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

trait LogsWriter {

  def write(e: LogEvent): Unit

  def close(): Unit
}

class Slf4jWriter(sourceId: String) extends LogsWriter {

  val sl4jLogger = LoggerFactory.getLogger(sourceId)

  def write(e: LogEvent): Unit = e.typedLevel match {
    case Level.Debug => sl4jLogger.debug(e.message)
    case Level.Info =>  sl4jLogger.info(e.message)
    case Level.Warn =>  sl4jLogger.warn(e.message)
    case Level.Error => sl4jLogger.error(e.message, e)
  }

  override def close(): Unit = ()

}

class RemoteLogsWriter(
  host: String,
  port: Int
) extends LogsWriter {

  private val sys = ActorSystem("logs-writer", RemoteLogsWriter.systemConfig)
  private val mat = ActorMaterializer()(sys)

  val kryoPool = {
    val inst = new ScalaKryoInstantiator
    inst.setRegistrationRequired(false)
    KryoPool.withByteArrayOutputStream(10, inst)
  }

  val inputActor = {
    def encode(a: Any): ByteString = {
      val body = ByteString(kryoPool.toBytesWithoutClass(a))
      val length = body.length
      val head = new Array[Byte](4)
      head(0) = (length >> 24).toByte
      head(1) = (length >> 16).toByte
      head(2) = (length >> 8).toByte
      head(3) = length.toByte

      ByteString(head) ++ body
    }

    Source.actorRef[LogEvent](1000, OverflowStrategy.dropHead)
      .map(encode)
      .via(Tcp()(sys).outgoingConnection(host, port))
      .toMat(Sink.ignore)(Keep.left).run()(mat)
  }

  def write(e: LogEvent): Unit = {
    inputActor ! e
  }

  def close(): Unit = {
    mat.shutdown()
    RemoteLogsWriter.remove(Key(host, port))
    Await.result(sys.terminate(), Duration.Inf)
  }

}

object RemoteLogsWriter {

  val systemConfig = ConfigFactory.parseString(
    "akka.daemonic = on"
  )

  @transient
  private val writers = new ConcurrentHashMap[Key, RemoteLogsWriter]()

  def getOrCreate(host: String, port: Int): RemoteLogsWriter = {
    getWith(host, port)(key => new RemoteLogsWriter(key.host, key.port))
  }

  def getWith(host: String, port: Int)(createFn: Key => RemoteLogsWriter): RemoteLogsWriter = {
    val key = Key(host, port)
    writers.computeIfAbsent(key, new java.util.function.Function[Key, RemoteLogsWriter] {
      override def apply(k: Key): RemoteLogsWriter = createFn(k)
    })
  }

  def remove(key: Key): RemoteLogsWriter = writers.remove(key)

  private case class Key(host: String, port: Int)

}

