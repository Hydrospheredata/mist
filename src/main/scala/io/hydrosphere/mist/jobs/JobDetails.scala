package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.Messages.JobMessages.JobParams
import org.joda.time.DateTime

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

    def apply(string: String): Status = string match {
      case "initialized" => Initialized
      case "queued" => Queued
      case "started" => Started
      case "finished" => Finished
      case "canceled" => Canceled
      case "failed" => Failed
      case x => throw new IllegalArgumentException(s"Unknown status $x")
    }

    case object Initialized extends Status with InProgress {
      override def toString: String = "initialized"
    }
    case object Queued extends Status with InProgress {
      override def toString: String = "queued"
    }
    case object Started extends Status with InProgress {
      override def toString: String = "started"
    }
    case object Finished extends Status with Done {
      override def toString: String = "finished"
    }
    case object Canceled extends Status with Done {
      override def toString: String = "canceled"
    }
    case object Failed extends Status with Done {
      override def toString: String = "failed"
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
  * @param endpoint - name of endpoint(route)
  * @param jobId - uniqId
  * @param params - filePath, className, args
  * @param context - target context/namespace
  * @param externalId - optional marker
  * @param source - run request source
  */
case class JobDetails(
  endpoint: String,
  jobId: String,
  params: JobParams,
  context: String,
  externalId: Option[String],
  source: JobDetails.Source,
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  jobResult: Option[Either[String, Map[String, Any]]] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized,
  workerId: String,
  createTime: Long = System.currentTimeMillis()
) {

  def withStartTime(time: Long): JobDetails = copy(startTime = Some(time))

  def starts(): JobDetails = withStartTime(new DateTime().getMillis)

  def withEndTime(time: Long): JobDetails = copy(endTime = Some(time))
  
  def ends(): JobDetails = withEndTime(new DateTime().getMillis)

  def withJobResult(result: Map[String, Any]): JobDetails =
    copy(jobResult = Some(Right(result)))

  def withFailure(message: String): JobDetails =
    copy(jobResult = Some(Left(message)))

  def withStatus(status: JobDetails.Status): JobDetails =
    copy(status = status)

  def isCancellable: Boolean = !status.isFinished
}

