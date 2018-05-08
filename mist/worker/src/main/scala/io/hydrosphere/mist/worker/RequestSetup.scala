package io.hydrosphere.mist.worker

import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.worker.logging.{LogsWriter, RemoteAppender, RemoteLogsWriter}
import org.apache.log4j.{LogManager, Logger}

object RequestSetup {

  type ReqSetup = RunJobRequest => RunJobRequest => Unit

  val NOOP: ReqSetup = (_ : RunJobRequest) => (_: RunJobRequest) => ()

  def loggingSetup(logger: Logger, writer: LogsWriter): ReqSetup = {
    (req: RunJobRequest) => {
      val app = RemoteAppender.create(req.id, writer)
      logger.addAppender(app)
      (req: RunJobRequest) => logger.removeAppender(req.id)
    }
  }

  def loggingSetup(writer: LogsWriter): ReqSetup = loggingSetup(LogManager.getRootLogger, writer)
}
