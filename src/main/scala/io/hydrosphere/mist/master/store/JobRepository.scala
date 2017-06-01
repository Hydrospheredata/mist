package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.jobs.JobDetails

import scala.concurrent.Future

trait JobRepository {

  def remove(jobId: String): Future[Unit]

  def get(jobId: String): Future[Option[JobDetails]]

  def getByExternalId(id: String): Future[Seq[JobDetails]]

  def getByEndpointId(id: String): Future[Seq[JobDetails]]

  def update(jobDetails: JobDetails): Future[Unit]

  def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]

  def all(): Future[Seq[JobDetails]]

  def clear(): Future[Unit]

}

