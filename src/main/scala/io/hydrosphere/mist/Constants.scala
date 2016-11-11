package io.hydrosphere.mist

object Constants {
  object Errors {
    final val jobTimeOutError = "Job timeout error"
    final val notJobSubclass = "External module is not MistJob subclass"
    final val extensionError = "You must specify the path to .jar or .py file"
  }
  object Actors {
    final val syncJobRunnerName = "SyncJobRunner"
    final val asyncJobRunnerName = "AsyncJobRunner"
    final val workerManagerName = "WorkerManager"
    final val mqttServiceName = "MQTTService"
    final val contextNode = "ContextNode"
  }
}
