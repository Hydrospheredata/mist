package io.hydrosphere.mist.api

import io.hydrosphere.mist.api.logging.MistLogging.{LogEvent, LogsWriter, RemoteLogsWriter, Slf4jWriter}

trait Logging extends ContextSupport {

  private var loggingConf: Option[CentralLoggingConf] = None
  private var jobId: String = _

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    this.loggingConf = conf.loggingConf
    this.jobId = conf.info.id
  }

  def getLogger: MLogger = MLogger(jobId, loggingConf)

  override private[mist] def stop(): Unit = {
    super.stop()
  }

}

case class MLogger(
  sourceId: String,
  centralConf: Option[CentralLoggingConf]
) {

  @transient
  lazy val writer = centralConf match {
    case Some(conf) => new LogsWriter {
      import conf._

      val slf4j = new Slf4jWriter(sourceId)
      val remote = RemoteLogsWriter.getOrCreate(host, port)

      override def write(e: LogEvent): Unit = {
        slf4j.write(e)
        remote.write(e)
      }

    }

    case None =>
      val slf4j = new Slf4jWriter(sourceId)
      slf4j.write(LogEvent.mkInfo(sourceId, "Central logging is not configured, using default slf4j"))
      slf4j
  }

  def debug(msg: String): Unit = {
    val e = LogEvent.mkDebug(sourceId, msg)
    writer.write(e)
  }

  def info(msg: String): Unit = {
    val e = LogEvent.mkInfo(sourceId, msg)
    writer.write(e)
  }

  def warn(msg: String): Unit = {
    val e = LogEvent.mkWarn(sourceId, msg)
    writer.write(e)
  }

  def error(msg: String, t: Throwable): Unit = {
    val e = LogEvent.mkError(sourceId, msg, t)
    writer.write(e)
  }
}

