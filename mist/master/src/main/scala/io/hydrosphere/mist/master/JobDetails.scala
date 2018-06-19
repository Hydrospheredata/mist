package io.hydrosphere.mist.master

import io.hydrosphere.mist.core.CommonData.JobParams
import io.hydrosphere.mist.master.JobDetails.Status
import io.hydrosphere.mist.master.Messages.StatusMessages._
import mist.api.data.JsData

object JobDetails {

  sealed trait Status{
    val isFinished: Boolean
  }
  
  sealed trait Done {
    val isFinished = true
  }
  
  sealed trait InProgress {
    val isFinished = false
  }

  object Status {
    object Keys {
      val initialized = "initialized"
      val queued = "queued"
      val started = "started"
      val jobFileDownloading = "job-file-downloading"
      val cancelling = "cancelling"
      val finished = "finished"
      val cancelled = "cancelled"
      val failed = "failed"
    }

    def apply(string: String): Status = string match {
      case Keys.initialized => Initialized
      case Keys.queued => Queued
      case Keys.started => Started
      case Keys.jobFileDownloading => FileDownloading
      case Keys.cancelling => Cancelling
      case Keys.finished => Finished
      case Keys.cancelled => Canceled
      case Keys.failed => Failed
      case x => throw new IllegalArgumentException(s"Unknown status $x")
    }

    val inProgress = Seq(
      Initialized,
      Queued,
      Started,
      FileDownloading,
      Cancelling
    )

    case object Initialized extends Status with InProgress {
      override def toString: String = Keys.initialized
    }
    case object Queued extends Status with InProgress {
      override def toString: String = Keys.queued
    }
    case object Started extends Status with InProgress {
      override def toString: String = Keys.started
    }
    case object FileDownloading extends Status with InProgress {
      override def toString: String = Keys.jobFileDownloading
    }
    case object Cancelling extends Status with InProgress {
      override def toString: String = Keys.cancelling
    }
    case object Finished extends Status with Done {
      override def toString: String = Keys.finished
    }
    case object Canceled extends Status with Done {
      override def toString: String = Keys.cancelled
    }
    case object Failed extends Status with Done {
      override def toString: String = Keys.failed
    }

  }

  sealed trait Source
  
  object Source {
    
    def apply(s: String): Source = s match {
      case "Http" => Http
      case "Cli" => Cli
      case x if x.startsWith("Async") =>
        val provider = x.split(" ").last
        Async(provider)
      case x => throw new IllegalArgumentException(s"Unknown Source $s")
    }
    
    case object Http extends Source {
      override def toString: String = "Http"
    }
    case object Cli extends Source {
      override def toString: String = "Cli"
    }
    case class Async(provider: String) extends Source {
      override def toString: String = s"Async $provider"
    }
    
  }
  
}

/**
  * Full information about job invocation
  *
  * @param function - name of function(route)
  * @param jobId - uniqId
  * @param params - filePath, className, args
  * @param context - target context/namespace
  * @param externalId - optional marker
  * @param source - run request source
  */
case class JobDetails(
  function: String,
  jobId: String,
  params: JobParams,
  context: String,
  externalId: Option[String],
  source: JobDetails.Source,
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  jobResult: Option[Either[String, JsData]] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized,
  workerId: Option[String] = None,
  createTime: Long = System.currentTimeMillis()
) {

  def withStartTime(time: Long): JobDetails = copy(startTime = Some(time))

  //TODO: should we use GMT0 here?
  def starts(): JobDetails = withStartTime(System.currentTimeMillis())

  def withEndTime(time: Long): JobDetails = copy(endTime = Some(time))

  def ends(): JobDetails = withEndTime(System.currentTimeMillis())

  def withJobResult(result: JsData): JobDetails =
    copy(jobResult = Some(Right(result)))

  def withFailure(message: String): JobDetails =
    copy(jobResult = Some(Left(message)))

  def withStatus(status: JobDetails.Status): JobDetails =
    copy(status = status)

  def isCancellable: Boolean = !status.isFinished

}

