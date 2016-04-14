package io.hydrosphere.mist

object Constants {

  object Errors {

    final lazy val jobTimeOutError = "Job timeout error"
    final lazy val noDoStuffMethod = "No overridden doStuff method"
    final lazy val notJobSubclass = "External module is not MistJob subclass"
    final lazy val extensionError = "You must specify the path to .jar or .py file"

  }

  object Actors {

    final lazy val syncJobRunnerName = "SyncJobRunner"
    final lazy val asyncJobRunnerName = "AsyncJobRunner"
    final lazy val contextManagerName = "ContextManager"
    final lazy val mqttServiceName = "MQTTService"

  }

}
