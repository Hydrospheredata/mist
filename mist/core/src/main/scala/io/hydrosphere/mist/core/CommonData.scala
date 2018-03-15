package io.hydrosphere.mist.core

import akka.actor.ActorRef
import mist.api.data.JsLikeData

import scala.concurrent.duration.Duration

object CommonData {

  case class WorkerReady(
    id: String,
    sparkUi: Option[String]
  )
  case class WorkerStartFailed(id: String, message: String)

  case object ConnectionUnused
  sealed trait ShutdownCommand
  case object CompleteAndShutdown extends ShutdownCommand
  case object ForceShutdown extends ShutdownCommand

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
    arguments: Map[String, Any],
    action: Action
  )

  case class RunJobRequest(
    id: String,
    params: JobParams,
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

  case class JobSuccess(id: String, result: JsLikeData) extends JobResponse
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

  sealed trait JobInfoMessage

  sealed trait InfoRequest extends JobInfoMessage {
    val className: String
    val jobPath: String
    val name: String
  }
  case class GetFunctionInfo(
    className: String,
    jobPath: String,
    name: String
  ) extends InfoRequest

  case class ValidateFunctionParameters(
    className: String,
    jobPath: String,
    name: String,
    params: Map[String, Any]
  ) extends InfoRequest

  case class GetAllFunctions(
    //TODO: find out why akka messages requires List but fails for Seq
    requests: List[GetFunctionInfo]
  ) extends JobInfoMessage

  case object EvictCache extends JobInfoMessage
  case object GetCacheSize extends JobInfoMessage

}
