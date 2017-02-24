package io.hydrosphere.mist.jobs

import java.util.UUID

import io.hydrosphere.mist.master.async.AsyncInterface.Provider
import org.joda.time.DateTime

object JobDetails {
  
  sealed trait Status
  
  object Status {
    
    def apply(string: String): Status = string match {
      case "Initialized" => Initialized
      case "Queued" => Queued
      case "Running" => Running
      case "Stopped" => Stopped
      case "Aborted" => Aborted
      case "Error" => Error
    }

    case object Initialized extends Status {
      override def toString: String = "Initialized"
    }
    case object Queued extends Status {
      override def toString: String = "Queued"
    }
    case object Running extends Status {
      override def toString: String = "Running"
    }
    case object Stopped extends Status {
      override def toString: String = "Stopped"
    }
    case object Aborted extends Status {
      override def toString: String = "Aborted"
    }
    case object Error extends Status {
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
                       configuration: FullJobConfiguration,
                       source: JobDetails.Source,
                       jobId: String = UUID.randomUUID().toString,
                       startTime: Option[Long] = None,
                       endTime: Option[Long] = None,
                       jobResult: Option[Either[Map[String, Any], String]] = None,
                       status: JobDetails.Status = JobDetails.Status.Initialized
                     )
{
  override def equals(that: Any): Boolean = that match {
    case t: JobDetails => t.jobId == jobId
    case _ => false
  }
  
  def withStartTime(time: Long): JobDetails = {
    JobDetails(configuration, source, jobId, Some(time), endTime, jobResult, status)
  }
  
  def starts(): JobDetails = {
    withStartTime(new DateTime().getMillis)
  }
  
  def withEndTime(time: Long): JobDetails = {
    JobDetails(configuration, source, jobId, startTime, Some(time), jobResult, status)
  }
  
  def ends(): JobDetails = {
    withEndTime(new DateTime().getMillis)
  }
  
  def withJobResult(result: Either[Map[String, Any], String]): JobDetails = {
    JobDetails(configuration, source, jobId, startTime, endTime, Some(result), status)
  }

  def withStatus(status: JobDetails.Status): JobDetails = {
    JobDetails(configuration, source, jobId, startTime, endTime, jobResult, status)
  }
}
