package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.jobs.JobDetails

import scala.concurrent.Future

trait JobRepository {

  def remove(jobId: String): Future[Unit]

  def get(jobId: String): Future[Option[JobDetails]]

  def getByEndpointId(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]
  def getByEndpointId(id: String, limit: Int, offset: Int): Future[Seq[JobDetails]] =
    getByEndpointId(id, limit, offset, Seq.empty)

  def update(jobDetails: JobDetails): Future[Unit]

  def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]

  def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]
  def getAll(limit: Int, offset: Int): Future[Seq[JobDetails]] = getAll(limit, offset, Seq.empty)

  def clear(): Future[Unit]

}

