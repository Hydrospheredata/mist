package io.hydrosphere.mist.jobs.store

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails

private[mist] trait JobRepository {
  def add(jobDetails: JobDetails): Unit
  def remove(jobId: String): Unit
  def get(jobId: String): Option[JobDetails]
  def getAll: List[JobDetails]
  def size: Int
  def clear(): Unit
  def update(jobDetails: JobDetails): Unit
  def filteredByStatuses(statuses: List[JobDetails.Status]): List[JobDetails]
}

object JobRepository {

  def apply(): JobRepository = {
    MistConfig.Recovery.recoveryTypeDb match {
      case "MapDb" => MapDbJobRepository
      case _ => InMemoryJobRepository
    }
  }
  
}