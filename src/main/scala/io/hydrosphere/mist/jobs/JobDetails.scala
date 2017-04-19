package io.hydrosphere.mist.jobs

import java.util.UUID

import io.hydrosphere.mist.master.interfaces.async.AsyncInterface.Provider
import io.hydrosphere.mist.utils.TypeAlias.JobResponseOrError
import org.joda.time.DateTime



object JobDetails {
  
  sealed trait Status{
  val isFinished: Boolean
  }
  
  sealed trait Finished {
    val isFinished = true
  }
  
  sealed trait NotFinished {
    val isFinished = false
  }
  object Status {

    def apply(string: String): Status = string match {
      case "Initialized" => Initialized
      case "Queued" => Queued
      case "Running" => Running
      case "Stopped" => Stopped
      case "Aborted" => Aborted
      case "Error" => Error
    }

    case object Initialized extends Status with NotFinished {
      override def toString: String = "Initialized"
    }
    case object Queued extends Status with NotFinished {
      override def toString: String = "Queued"
    }
    case object Running extends Status with NotFinished {
      override def toString: String = "Running"
    }
    case object Stopped extends Status with Finished {
      override def toString: String = "Stopped"
    }
    case object Aborted extends Status with Finished {
      override def toString: String = "Aborted"
    }
    case object Error extends Status with Finished {
      override def toString: String = "Error"
    }

  }

  sealed trait Source
  
  object Source {
    
    def apply(string: String): Source = string match {
      case "Http" => Http
      case "Cli" => Cli
      case async if async.startsWith("Async") => Async(Provider(async.split(" ").last))
    }
    
    case object Http extends Source {
      override def toString: String = "Http"
    }
    case object Cli extends Source {
      override def toString: String = "Cli"
    }
    case class Async(provider: Provider) extends Source {
      override def toString: String = s"Async ${provider.toString}"
    }
    
  }
  
}

case class JobDetails(
  configuration: JobExecutionParams,
  source: JobDetails.Source,
  jobId: String = UUID.randomUUID().toString,
  startTime: Option[Long] = None,
  endTime: Option[Long] = None,
  jobResult: Option[JobResponseOrError] = None,
  status: JobDetails.Status = JobDetails.Status.Initialized
) {

  def withStartTime(time: Long): JobDetails = copy(startTime = Some(time))

  def starts(): JobDetails = withStartTime(new DateTime().getMillis)

  def withEndTime(time: Long): JobDetails = copy(endTime = Some(time))
  
  def ends(): JobDetails = withEndTime(new DateTime().getMillis)

  def withJobResult(result: JobResponseOrError): JobDetails =
    copy(jobResult = Some(result))

  def withStatus(status: JobDetails.Status): JobDetails =
    copy(status = status)
}

