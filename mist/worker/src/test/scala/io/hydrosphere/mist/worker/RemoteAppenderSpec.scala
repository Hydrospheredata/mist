package io.hydrosphere.mist.worker

import mist.api.logging.MistLogging
import mist.api.logging.MistLogging.{LogEvent, LogsWriter}
import io.hydrosphere.mist.core.MockitoSugar
import mist.api.CentralLoggingConf
import org.apache.log4j.spi.{LoggingEvent, NOPLogger, NOPLoggerRepository}
import org.apache.log4j.{Category, Level}
import org.mockito.Matchers.{eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.{FunSpecLike, Matchers}

class RemoteAppenderSpec extends FunSpecLike with Matchers with MockitoSugar {


  val noopLogger = new NOPLogger(new NOPLoggerRepository, "forTest")

  it("should create appender with layout") {
    val appender = RemoteAppender("id", CentralLoggingConf("localhost", 2005))
    appender.getLayout should not be null
    appender.close()
  }

  it("should send message to logs writer with correct level") {

    val logsWriter = mock[LogsWriter]

    doNothing()
      .when(logsWriter).write(any[LogEvent])

    val appender = RemoteAppender.create("id", logsWriter)

    appender.append(mkLoggingEvent(Level.INFO, "info"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "info", 1L, MistLogging.Level.Info.value, None)))

    appender.append(mkLoggingEvent(Level.DEBUG, "debug"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "debug", 1L, MistLogging.Level.Debug.value, None)))

    appender.append(mkLoggingEvent(Level.WARN, "warn"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "warn", 1L, MistLogging.Level.Warn.value, None)))

    appender.append(mkErrorLoggingEvent("error", None))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "error", 1L, MistLogging.Level.Error.value, None)))

    val error = Some(new RuntimeException("test"))
    appender.append(mkErrorLoggingEvent("error", error))
    verify(logsWriter).write(mockitoEq(
      LogEvent.mkError("id", "error", error, 1L)
    ))

    appender.append(mkLoggingEvent(Level.FATAL, "unknown level"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "FATAL - unknown level\n", 1L, MistLogging.Level.Info.value, None)))

  }

  def mkLoggingEvent(level: Level, msg: String): LoggingEvent =
    new LoggingEvent(classOf[Category].getName, noopLogger, 1L, level, msg, null)

  def mkErrorLoggingEvent(msg: String, ex: Option[Throwable]): LoggingEvent = {
    new LoggingEvent(classOf[Category].getName, noopLogger, 1L, Level.ERROR, msg, ex.orNull)
  }
}
