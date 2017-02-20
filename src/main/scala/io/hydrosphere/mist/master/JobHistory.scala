package io.hydrosphere.mist.master

import akka.actor.{Actor, Props}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.utils.Logger

object JobHistory {
  
  def props(): Props = Props(classOf[JobHistory])
  
  case class AddJob(jobDetails: JobDetails)
  case class UpdateJob(jobDetails: JobDetails)
}

class JobHistory extends Actor with Logger {
  override def receive: Receive = {
    case JobHistory.AddJob(job: JobDetails) =>
      logger.debug(s"Adding new job (#${job.jobId}) to history with ${job.status.toString} status")
      JobRepository().add(job)

    case JobHistory.UpdateJob(job: JobDetails) =>
      logger.debug(s"Updating existing job (#${job.jobId}) in history with ${job.status.toString} status")
      JobRepository().update(job)
      logger.debug(s"UPDATED: ${JobRepository().get(job.jobId).toString}")
  }
}
