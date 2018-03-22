package io.hydrosphere.mist.worker

import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.api.logging.MistLogging
import io.hydrosphere.mist.api.logging.MistLogging.RemoteLogsWriter
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, SimpleLayout}

class RemoteAppender(sourceId: String, logsWriter: RemoteLogsWriter) extends AppenderSkeleton {

  override def append(event: LoggingEvent): Unit = {

    val evt = event.getLevel match {
      case Level.INFO => MistLogging.LogEvent.mkInfo(sourceId, event.getRenderedMessage)
      case Level.DEBUG => MistLogging.LogEvent.mkDebug(sourceId, event.getRenderedMessage)
      case Level.ERROR =>
        MistLogging.LogEvent.mkError(
          sourceId,
          event.getRenderedMessage,
          Option(event.getThrowableInformation).map(_.getThrowable)
        )
      case Level.WARN => MistLogging.LogEvent.mkWarn(sourceId, event.getRenderedMessage)
      case _ => MistLogging.LogEvent.mkInfo(sourceId, this.getLayout.format(event))
    }
    logsWriter.write(evt)
  }

  override def close(): Unit = logsWriter.close()

  override def requiresLayout(): Boolean = true
}


object RemoteAppender {
  def apply(sourceId: String, loggingConf: CentralLoggingConf): RemoteAppender = {
    val jobLogsAppender = new RemoteAppender(
      sourceId,
      RemoteLogsWriter.getOrCreate(loggingConf.host, loggingConf.port)
    )
    jobLogsAppender.setLayout(new SimpleLayout)
    jobLogsAppender.setName(sourceId)
    jobLogsAppender
  }
}