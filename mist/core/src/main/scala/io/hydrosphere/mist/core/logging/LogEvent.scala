package io.hydrosphere.mist.core.logging

import java.io.{PrintWriter, StringWriter}
import java.time.format.DateTimeFormatter
import java.time._

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

  def mkError(from: String, message: String, t: Throwable): LogEvent =
    mkError(from, message, Some(t))

  def mkError(from: String, message: String, optT: Option[Throwable] = None, ts: Long = mkTimestamp): LogEvent = {
    val errTrace = optT.map(ex => {
      val writer = new StringWriter()
      ex.printStackTrace(new PrintWriter(writer))
      writer.toString
    })

    LogEvent(from, message, ts, Level.Error.value, errTrace)
  }

  def nonFatal(level: Level, from: String, message: String, ts: Long): LogEvent =
    LogEvent(from, message, ts, level.value, None)

  private def mkTimestamp: Long =
    LocalDateTime.now(ZoneOffset.UTC).toInstant(ZoneOffset.UTC).toEpochMilli
}

