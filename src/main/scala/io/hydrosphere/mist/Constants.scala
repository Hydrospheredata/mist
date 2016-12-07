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
  object CLI {
    final val stopWorkerMsg = "kill worker"
    final val stopJobMsg = "kill job"
    final val listWorkersMsg = "list workers"
    final val listJobsMsg = "list jobs"
    final val stopAllWorkersMsg = "kill all"
    final val exitMsg = "exit"
    final val jobMsgMarker = "[J]"
    final val cliActorName = "CLI"

    final val noWorkersMsg = "no workers"
  }
}
