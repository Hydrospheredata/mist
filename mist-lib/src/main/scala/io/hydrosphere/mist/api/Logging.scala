package io.hydrosphere.mist.api

import io.hydrosphere.mist.api.logging.MistLogging.{LogEvent, Slf4jWriter}

trait Logging extends ContextSupport {

  private var loggingConf: Option[CentralLoggingConf] = None
  private var jobId: String = _

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    this.loggingConf = conf.loggingConf
    this.jobId = conf.info.id
  }

  def getLogger: MLogger = new MLogger(jobId)

  override private[mist] def stop(): Unit = {
    super.stop()
  }

}

class MLogger(sourceId: String) {

  @transient
  lazy val writer = new Slf4jWriter(sourceId)

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

