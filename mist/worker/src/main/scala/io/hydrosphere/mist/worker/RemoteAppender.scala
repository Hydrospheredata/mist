package io.hydrosphere.mist.worker

import io.hydrosphere.mist.api.CentralLoggingConf
import io.hydrosphere.mist.api.logging.MistLogging
import io.hydrosphere.mist.api.logging.MistLogging.{LogsWriter, RemoteLogsWriter}
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{AppenderSkeleton, Level, SimpleLayout}

class RemoteAppender(sourceId: String, logsWriter: LogsWriter) extends AppenderSkeleton {

  override def append(event: LoggingEvent): Unit = {

    val timeStamp = event.timeStamp
    val message = event.getRenderedMessage
    val evt = event.getLevel match {
      case Level.INFO => MistLogging.LogEvent.mkInfo(sourceId, message, timeStamp)
      case Level.DEBUG => MistLogging.LogEvent.mkDebug(sourceId, message, timeStamp)
      case Level.ERROR =>
        MistLogging.LogEvent.mkError(
          sourceId, message,
          Option(event.getThrowableInformation).map(_.getThrowable),
          timeStamp
        )
      case Level.WARN => MistLogging.LogEvent.mkWarn(sourceId, message, timeStamp)
      case _ => MistLogging.LogEvent.mkInfo(sourceId, this.getLayout.format(event), timeStamp)
    }
    logsWriter.write(evt)
  }

  override def close(): Unit = logsWriter.close()

  override def requiresLayout(): Boolean = true
}


object RemoteAppender {
  def apply(sourceId: String, loggingConf: CentralLoggingConf): RemoteAppender =
    create(sourceId, RemoteLogsWriter.getOrCreate(loggingConf.host, loggingConf.port))

  def create(sourceId: String, logsWriter: LogsWriter): RemoteAppender = {
    val jobLogsAppender = new RemoteAppender(sourceId, logsWriter)
    jobLogsAppender.setLayout(new SimpleLayout)
    jobLogsAppender.setName(sourceId)
    jobLogsAppender
  }

}