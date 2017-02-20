package io.hydrosphere.mist.jobs

import org.joda.time.DateTime

/*
object SourceType {
  val HTTP
  val Kafka
  val MQTT
  val CLI
}

 */

object JobDetails {
  
  sealed trait Status
  
  object Status {
    
    def apply(string: String): Status = string match {
      case "INITIALIZED" => INITIALIZED
      case "QUEUED" => QUEUED
      case "RUNNING" => RUNNING
      case "STOPPED" => STOPPED
      case "ABORTED" => ABORTED
      case "ERROR" => ERROR
    }

    case object INITIALIZED extends Status {
      override def toString: String = "INITIALIZED"
    }
    case object QUEUED extends Status {
      override def toString: String = "QUEUED"
    }
    case object RUNNING extends Status {
      override def toString: String = "RUNNING"
    }
    case object STOPPED extends Status {
      override def toString: String = "STOPPED"
    }
    case object ABORTED extends Status {
      override def toString: String = "ABORTED"
    }
    case object ERROR extends Status {
      override def toString: String = "ERROR"
    }

  }
  
}

case class JobDetails(
                       configuration: FullJobConfiguration,
                       jobId: String,
                       startTime: Option[Long] = None,
                       endTime: Option[Long] = None,
                       jobResult: Option[Either[Map[String, Any], String]] = None,
                       status: JobDetails.Status = JobDetails.Status.INITIALIZED
                       /* source: SourceType */
                     )
{
  override def equals(that: Any): Boolean = that match {
    case t: JobDetails => t.jobId == jobId
    case _ => false
  }
  
  def withStartTime(time: Long): JobDetails = {
    JobDetails(configuration, jobId, Some(time), endTime, jobResult, status)
  }
  
  def starts(): JobDetails = {
    withStartTime(new DateTime().getMillis)
  }
  
  def withEndTime(time: Long): JobDetails = {
    JobDetails(configuration, jobId, startTime, Some(time), jobResult, status)
  }
  
  def ends(): JobDetails = {
    withEndTime(new DateTime().getMillis)
  }
  
  def withJobResult(result: Either[Map[String, Any], String]): JobDetails = {
    JobDetails(configuration, jobId, startTime, endTime, Some(result), status)
  }

  def withStatus(status: JobDetails.Status): JobDetails = {
    JobDetails(configuration, jobId, startTime, endTime, jobResult, status)
  }
}
