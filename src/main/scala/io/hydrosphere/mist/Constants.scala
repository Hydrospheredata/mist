package io.hydrosphere.mist

import scala.concurrent.duration._

object Constants {
  object Errors {
    final val jobTimeOutError = "Job timeout error"
    final val notJobSubclass = "External module is not MistJob subclass"
    final val extensionError = "You must specify the path to .jar or .py file"
  }
  object Actors {
    final val syncJobRunnerName = "SyncJobRunner"
    final val asyncJobRunnerName = "AsyncJobRunner"
    final val clusterManagerName = "ClusterManager"
    final val mqttServiceName = "MQTTService"
    final val contextNode = "ContextNode"
  }
  object CLI {
    final val stopWorkerMsg = "kill worker"
    final val stopJobMsg = "kill job"
    final val listWorkersMsg = "list workers"
    final val listRoutersMsg = "list routers"
    final val listJobsMsg = "list jobs"
    final val stopAllWorkersMsg = "kill all"
    final val exitMsg = "exit"
    final val cliActorName = "CLI"
    final val startJob = "start job"
    final val noWorkersMsg = "no workers"
    final val internalUserInterfaceActorName = "InternalUIActor"
    final val timeoutDuration = 60.second
    final val stopAllWorkers = "All contexts are scheduled for shutdown."
  }
  object ML {
    object Models {
      final val randomForestClassifier = "org.apache.spark.ml.classification.RandomForestClassificationModel"
      final val randomForestRegressor = "org.apache.spark.ml.regression.RandomForestRegressionModel"
      final val naiveBayes = "org.apache.spark.ml.classification.NaiveBayesModel"
      final val gbtRegressor = "org.apache.spark.ml.regression.GBTRegressionModel"
    }
    object Columns {
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
