package io.hydrosphere.mist.master

import io.hydrosphere.mist.Messages.WorkerMessages.WorkerInitInfo
import io.hydrosphere.mist.master.data.ContextsStorage

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class InfoProvider(
  logServiceConfig: LogServiceConfig,
  contextsStorage: ContextsStorage
) {

  def workerInitInfo(contextName: String): Future[WorkerInitInfo] = {
    contextsStorage.getOrDefault(contextName)
      .map(contextConfig => WorkerInitInfo(
        sparkConf = contextConfig.sparkConf,
        maxJobs = contextConfig.maxJobs,
        downtime = contextConfig.downtime,
        streamingDuration = contextConfig.streamingDuration,
        logService = s"${logServiceConfig.host}:${logServiceConfig.port}"
      ))
  }
}
