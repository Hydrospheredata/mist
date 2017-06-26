package io.hydrosphere.mist.api.logging

import java.io.{PrintWriter, StringWriter}
import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source, Tcp}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.util.ByteString
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}

private [mist] object MistLogging {

  case class Level(value: Int, name: String)

  object Level {

    def fromInt(i: Int): Level = i match {
      case 1 => Debug
      case 2 => Info
      case 3 => Warn
      case 4 => Error
      case x => throw new IllegalArgumentException(s"Unknown level $i")
    }

    object Debug extends Level(1, "DEBUG")
    object Info extends Level(2, "INFO")
    object Warn extends Level(3, "WARN")
    object Error extends Level(4, "ERROR")
  }

  case class LogEvent(
    from: String,
    message: String,
    timeStamp: Long,
    level: Int,
    errTrace: Option[String]) {

    def mkString: String = {
      val date = formatDate
      val error = errTrace.map(s => "\n" + s).getOrElse("")
      s"${typedLevel.name} $date [$from] $message$error"
    }

    private def formatDate: String = {
      val inst = Instant.ofEpochMilli(timeStamp)
      val date = LocalDateTime.ofInstant(inst, ZoneOffset.UTC)
      DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(date)
    }

    def typedLevel: Level = Level.fromInt(level)
  }

  object LogEvent {

    def mkDebug(from: String, message: String, ts: Long = mkTimestamp): LogEvent =
      nonFatal(Level.Debug, from, message, ts)

    def mkInfo(from: String, message: String, ts: Long = mkTimestamp): LogEvent =
      nonFatal(Level.Info, from, message, ts)

    def mkWarn(from: String, message: String, ts: Long = mkTimestamp): LogEvent =
      nonFatal(Level.Warn, from, message, ts)

    def mkError(from: String, message: String, t: Throwable, ts: Long = mkTimestamp): LogEvent = {
      val writer = new StringWriter()
      t.printStackTrace(new PrintWriter(writer))
      val errTrace = writer.toString

      LogEvent(from, message, ts, Level.Error.value, Some(errTrace))
    }

    def nonFatal(level: Level, from: String, message: String, ts: Long): LogEvent =
      LogEvent(from, message, ts, level.value, None)

    private def mkTimestamp: Long =
      LocalDateTime.now(ZoneOffset.UTC).toInstant(ZoneOffset.UTC).toEpochMilli
  }

  trait LogsWriter {

    def write(e: LogEvent): Unit

  }

  class Slf4jWriter(sourceId: String) extends LogsWriter {

    val sl4jLogger = LoggerFactory.getLogger(sourceId)

    def write(e: LogEvent): Unit = e.typedLevel match {
      case Level.Debug => sl4jLogger.debug(e.message)
      case Level.Info =>  sl4jLogger.info(e.message)
      case Level.Warn =>  sl4jLogger.warn(e.message)
      case Level.Error => sl4jLogger.error(e.message, e)
    }
  }

  class RemoteLogsWriter(host: String, port: Int) extends LogsWriter {

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
      sys.shutdown()
    }

  }

  object RemoteLogsWriter {

    val systemConfig = ConfigFactory.parseString(
      "akka.daemonic = on"
    )

    @transient
    private val writers = new ConcurrentHashMap[Key, RemoteLogsWriter]()

    def getOrCreate(host: String, port: Int): RemoteLogsWriter = {
      val key = Key(host, port)
      writers.computeIfAbsent(key, new java.util.function.Function[Key, RemoteLogsWriter] {
        override def apply(k: Key): RemoteLogsWriter = {
          new RemoteLogsWriter(k.host, k.port)
        }
      })
    }

    private case class Key(host: String, port: Int)

  }
}
