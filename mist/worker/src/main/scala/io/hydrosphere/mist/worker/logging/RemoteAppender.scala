package io.hydrosphere.mist.worker.logging

import io.hydrosphere.mist.core.logging.LogEvent
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, SimpleLayout}

class RemoteAppender(sourceId: String, logsWriter: LogsWriter) extends AppenderSkeleton {

  override def append(event: LoggingEvent): Unit = {

    val timeStamp = event.timeStamp
    val message = event.getRenderedMessage
    val evt = event.getLevel match {
      case Level.INFO => LogEvent.mkInfo(sourceId, message, timeStamp)
      case Level.DEBUG => LogEvent.mkDebug(sourceId, message, timeStamp)
      case Level.ERROR =>
        LogEvent.mkError(
          sourceId, message,
          Option(event.getThrowableInformation).map(_.getThrowable),
          timeStamp
        )
      case Level.WARN => LogEvent.mkWarn(sourceId, message, timeStamp)
      case _ => LogEvent.mkInfo(sourceId, this.getLayout.format(event), timeStamp)
    }
    logsWriter.write(evt)
  }

  override def close(): Unit = ()

  override def requiresLayout(): Boolean = true
}


object RemoteAppender {

  def create(sourceId: String, logsWriter: LogsWriter): RemoteAppender = {
    val jobLogsAppender = new RemoteAppender(sourceId, logsWriter)
    jobLogsAppender.setLayout(new SimpleLayout)
    jobLogsAppender.setName(sourceId)
    jobLogsAppender
  }

}