package io.hydrosphere.mist.jobs.store

import io.hydrosphere.mist.jobs.JobDetails

private[mist] object InMemoryJobRepository extends JobRepository {

  private val _collection = scala.collection.mutable.Map[String, JobDetails]()

  override def add(jobDetails: JobDetails): Unit = {
    _collection put (jobDetails.jobId, jobDetails)
  }

  override def remove(jobId: String): Unit = {
    _collection.remove(jobId)
  }

  override def get(jobId: String): Option[JobDetails] = {
    _collection.get(jobId)
  }
  
  override def getAll: List[JobDetails] = _collection.values.toList

  override def size: Int = _collection.size

  override def clear(): Unit = {
    _collection.clear()
  }

  override def update(jobDetails: JobDetails): Unit = {
    add(jobDetails)
  }
}
