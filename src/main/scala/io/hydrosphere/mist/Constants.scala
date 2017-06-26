package io.hydrosphere.mist

import scala.concurrent.duration._

object Constants {
  object Actors {
    val syncJobRunnerName = "SyncJobRunner"
    val asyncJobRunnerName = "AsyncJobRunner"
    val clusterManagerName = "ClusterManager"
    val mqttServiceName = "MQTTService"
    val kafkaServiceName = "KafkaService"
    val contextNode = "ContextNode"
    val cliName = "CLI"
    val cliResponderName = "CliResponder"
  }

  object CLI {
    object Commands {
      val help = "help"
      val stopWorker = "kill worker"
      val stopJob = "kill job"
      val listWorkers = "list workers"
      val listRouters = "list routers"
      val listJobs = "list jobs"
      val stopAllWorkers = "kill all"
      val exit = "exit"
      val startJob = "start job"
    }

    val noWorkersMsg = "no workers"
    val internalUserInterfaceActorName = "InternalUIActor"
    val timeoutDuration = 60.second
    val stopAllWorkers = "All contexts are scheduled for shutdown."
  }
}
