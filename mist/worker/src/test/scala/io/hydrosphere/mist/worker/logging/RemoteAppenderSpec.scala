package io.hydrosphere.mist.worker.logging

import io.hydrosphere.mist.core.MockitoSugar
import io.hydrosphere.mist.core.logging._
import org.apache.log4j.spi.{LoggingEvent, NOPLogger, NOPLoggerRepository}
import org.apache.log4j.{Category, Level => L4jLevel}
import org.mockito.Matchers.{eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.{FunSpecLike, Matchers}

class RemoteAppenderSpec extends FunSpecLike with Matchers with MockitoSugar {


  val noopLogger = new NOPLogger(new NOPLoggerRepository, "forTest")

  it("should create appender with layout") {
    val appender = RemoteAppender.create("id", new Slf4jWriter("id"))
    appender.getLayout should not be null
    appender.close()
  }

  it("should send message to logs writer with correct level") {

    val logsWriter = mock[LogsWriter]

    doNothing()
      .when(logsWriter).write(any[LogEvent])

    val appender = RemoteAppender.create("id", logsWriter)

    appender.append(mkLoggingEvent(L4jLevel.INFO, "info"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "info", 1L, Level.Info.value, None)))

    appender.append(mkLoggingEvent(L4jLevel.DEBUG, "debug"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "debug", 1L, Level.Debug.value, None)))

    appender.append(mkLoggingEvent(L4jLevel.WARN, "warn"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "warn", 1L, Level.Warn.value, None)))

    appender.append(mkErrorLoggingEvent("error", None))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "error", 1L, Level.Error.value, None)))

    val error = Some(new RuntimeException("test"))
    appender.append(mkErrorLoggingEvent("error", error))
    verify(logsWriter).write(mockitoEq(
      LogEvent.mkError("id", "error", error, 1L)
    ))

    appender.append(mkLoggingEvent(L4jLevel.FATAL, "unknown level"))
    verify(logsWriter).write(mockitoEq(LogEvent("id", "FATAL - unknown level\n", 1L, Level.Info.value, None)))

  }

  def mkLoggingEvent(level: L4jLevel, msg: String): LoggingEvent =
    new LoggingEvent(classOf[Category].getName, noopLogger, 1L, level, msg, null)

  def mkErrorLoggingEvent(msg: String, ex: Option[Throwable]): LoggingEvent = {
    new LoggingEvent(classOf[Category].getName, noopLogger, 1L, L4jLevel.ERROR, msg, ex.orNull)
  }
}
