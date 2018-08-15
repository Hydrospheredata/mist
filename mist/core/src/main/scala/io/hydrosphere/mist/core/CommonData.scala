package io.hydrosphere.mist.core

import akka.actor.ActorRef
import mist.api.data.{JsData, JsMap}

import scala.concurrent.duration.Duration

object CommonData {

  case class WorkerReady(
    id: String,
    sparkUi: Option[String]
  )
  case class WorkerStartFailed(id: String, message: String)

  // ask worker node to stop worker actor
  case object ShutdownWorker
  // ask worker node to stop self
  case object ShutdownWorkerApp
  // ask master to stop worker node - call user fn or send ShutdownWorkerApp
  case object RequestTermination
  // last word from worker node
  case object Goodbye

  /**
    * Data for creation spark/mist context on worker
   */
  case class WorkerInitInfo(
    sparkConf: Map[String, String],
    maxJobs: Int,
    downtime: Duration,
    streamingDuration: Duration,
    logService: String,
    masterAddress: String,
    masterHttpConf: String,
    maxArtifactSize: Long,
    runOptions: String
  ) {
    def isK8S: Boolean = sparkConf.exists({case (k, v) => k == "spark.master" && v.startsWith("k8s://")})
  }

  case class JobParams(
    filePath: String,
    className: String,
    arguments: JsMap,
    action: Action
  )

  case class RunJobRequest(
    id: String,
    params: JobParams,
    startTimeout: Duration = Duration.Inf,
    timeout: Duration = Duration.Inf
  )

  sealed trait RunJobResponse {
    val id: String
    val time: Long
  }

  case class JobStarted(
    id: String,
    time: Long = System.currentTimeMillis()
  ) extends RunJobResponse

  case class JobFileDownloading(
    id: String,
    time: Long = System.currentTimeMillis()
  ) extends RunJobResponse

  case class WorkerIsBusy(
    id: String,
    time: Long = System.currentTimeMillis()
  ) extends RunJobResponse


  case class CancelJobRequest(id: String)
  case class JobIsCancelled(
    id: String,
    time: Long = System.currentTimeMillis()
  )

  // internal messages
  sealed trait JobResponse {
    val id: String
  }

  case class JobSuccess(id: String, result: JsData) extends JobResponse
  case class JobFailure(id: String, error: String) extends JobResponse

  sealed trait GetRunInitInfo
  case object GetRunInitInfo extends GetRunInitInfo

  sealed trait Action
  object Action {

    def apply(string: String): Action = string match {
      case "execute" => Execute
      case "serve" => Serve
    }

    case object Execute extends Action {
      override def toString: String = "execute"
    }

    case object Serve extends Action {
      override def toString: String = "serve"
    }
  }

  val FunctionInfoProviderRegisterActorName = "job-info-provider-register"
  val HealthActorName = "health"

  case class RegisterJobInfoProvider(ref: ActorRef)

  case class EnvInfo(pythonSettings: PythonEntrySettings)

  sealed trait JobInfoMessage

  sealed trait InfoRequest extends JobInfoMessage {
    val className: String
    val jobPath: String
    val name: String
    val envInfo: EnvInfo
  }
  final case class GetFunctionInfo(
    className: String,
    jobPath: String,
    name: String,
    envInfo: EnvInfo
  ) extends InfoRequest

  final case class DeleteFunctionInfo(name: String) extends JobInfoMessage

  final case class ValidateFunctionParameters(
    className: String,
    jobPath: String,
    name: String,
    params: JsMap,
    envInfo: EnvInfo
  ) extends InfoRequest

  final case class GetAllFunctions(requests: List[GetFunctionInfo]) extends JobInfoMessage

  final case object EvictCache extends JobInfoMessage
  final case object GetCacheSize extends JobInfoMessage

}
