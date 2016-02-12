package com.provectus.lymph

object Constants {

  object Errors {

    final val jobTimeOutError = "Job timeout error"
    final val noDoStuffMethod = "No overridden doStuff method"
    final val notJobSubclass = "External module is not LymphJob subclass"

  }

  object Actors {

    final val syncJobRunnerName = "SyncJobRunner"
    final val asyncJobRunnerName = "AsyncJobRunner"
    final val contextManagerName = "ContextManager"
    final val mqttServiceName = "MQTTService"

  }

}
