package io.hydrosphere.mist

import scala.concurrent.duration._

object Constants {
  object Actors {
    final val syncJobRunnerName = "SyncJobRunner"
    final val asyncJobRunnerName = "AsyncJobRunner"
    final val clusterManagerName = "ClusterManager"
    final val mqttServiceName = "MQTTService"
    final val kafkaServiceName = "KafkaService"
    final val contextNode = "ContextNode"
    final val cliName = "CLI"
    final val cliResponderName = "CliResponder"
  }
  
  object CLI {
    object Commands {
      final val stopWorker = "kill worker"
      final val stopJob = "kill job"
      final val listWorkers = "list workers"
      final val listRouters = "list routers"
      final val listJobs = "list jobs"
      final val stopAllWorkers = "kill all"
      final val exit = "exit"
      final val startJob = "start job"
    }
    
    final val noWorkersMsg = "no workers"
    final val internalUserInterfaceActorName = "InternalUIActor"
    final val timeoutDuration = 60.second
    final val stopAllWorkers = "All contexts are scheduled for shutdown."
  }
  object ML {
    object Models {
      final val randomForestClassifier = "org.apache.spark.ml.classification.RandomForestClassificationModel"
    }
    object Collumns {
      final val inputCol = "inputCol"
      final val outputCol = "outputCol"
      final val featuresCol = "featuresCol"
      final val predictionCol = "predictionCol"
      final val probabilityCol = "probabilityCol"
      final val thresholds = "thresholds"
    }
    object Params {
      final val numFeatures = "numFeatures"
      final val numClasses = "numClasses"
    }
    final val binary = "binary"
    final val rootNode = "rootNode"
  }
}
