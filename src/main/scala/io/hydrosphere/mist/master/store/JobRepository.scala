package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.JobDetails

private[mist] trait JobRepository {
  def remove(jobId: String): Unit
  def get(jobId: String): Option[JobDetails]
  def getByExternalId(id: String): Option[JobDetails]
  def size: Long
  def clear(): Unit
  def update(jobDetails: JobDetails): Unit
  def filteredByStatuses(statuses: List[JobDetails.Status]): List[JobDetails]
  def queuedInNamespace(namespace: String): List[JobDetails]
  def runningInNamespace(namespace: String): List[JobDetails]
}

object JobRepository {

  def apply(): JobRepository = {
    MistConfig.History.dbType match {
      case "MapDb" => MapDbJobRepository
      case "Sqlite" => SqliteJobRepository
      case _ => InMemoryJobRepository
    }
  }
  
}