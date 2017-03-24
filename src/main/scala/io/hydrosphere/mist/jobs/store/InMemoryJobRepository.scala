package io.hydrosphere.mist.jobs.store

import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.JobDetails.Status

class InMemoryJobRepository extends JobRepository {

  private val _collection = scala.collection.mutable.Map[String, JobDetails]()

  private def add(jobDetails: JobDetails): Unit = {
    _collection put (jobDetails.jobId, jobDetails)
  }

  override def remove(jobId: String): Unit = {
    _collection.remove(jobId)
  }

  override def get(jobId: String): Option[JobDetails] = {
    _collection.get(jobId)
  }
  
  private def getAll: List[JobDetails] = _collection.values.toList

  override def size: Long = _collection.size.toLong

  override def clear(): Unit = {
    _collection.clear()
  }

  override def update(jobDetails: JobDetails): Unit = {
    add(jobDetails)
  }

  override def filteredByStatuses(statuses: List[Status]): List[JobDetails] = {
    getAll.filter {
      job: JobDetails => statuses contains job.status
    }
  }

  override def queuedInNamespace(namespace: String): List[JobDetails] = {
    getAll.filter {
      job => job.status == JobDetails.Status.Queued && job.configuration.namespace == namespace
    }
  }

  override def runningInNamespace(namespace: String): List[JobDetails] = {
    getAll.filter {
      job => job.status == JobDetails.Status.Running && job.configuration.namespace == namespace
    }
  }
}

object InMemoryJobRepository extends InMemoryJobRepository
