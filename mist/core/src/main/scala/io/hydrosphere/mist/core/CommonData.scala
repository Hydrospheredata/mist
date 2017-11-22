package io.hydrosphere.mist.core

import akka.actor.ActorRef
import mist.api.data.JsLikeData

import scala.concurrent.duration.Duration

object CommonData {

  /**
    * Request data for creating spark/mist context on worker
    */
  case class WorkerInitInfoReq(contextName: String)

  /**
    * Data for creation spark/mist context on worker
   */
  case class WorkerInitInfo(
    sparkConf: Map[String, String],
    maxJobs: Int,
    downtime: Duration,
    streamingDuration: Duration,
    logService: String,
    masterHttpConf: String,
    jobsSavePath: String
  )

  /**
    * Initial message to master when worker ready to work
    */
  case class WorkerRegistration(
    name: String,
    address: String,
    sparkUi: Option[String]
  )

  case class JobParams(
    filePath: String,
    className: String,
    arguments: Map[String, Any],
    action: Action
  )

  case class RunJobRequest(
    id: String,
    params: JobParams
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

  val JobInfoProviderRegisterActorName = "job-info-provider-register"
  case class RegisterJobInfoProvider(ref: ActorRef)

  sealed trait JobInfoMessage {
    val className: String
    val jobPath: String
  }
  case class GetJobInfo(className: String, jobPath: String) extends JobInfoMessage
  case class ValidateJobParameters(
    className: String,
    jobPath: String,
    action: Action,
    params: Map[String, Any]
  ) extends JobInfoMessage
}
