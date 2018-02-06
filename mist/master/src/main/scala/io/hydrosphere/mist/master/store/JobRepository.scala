package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.master.JobDetails

import scala.concurrent.Future

trait JobRepository {

  def remove(jobId: String): Future[Unit]

  def get(jobId: String): Future[Option[JobDetails]]

  def getByFunctionId(id: String, limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]
  def getByFunctionId(id: String, limit: Int, offset: Int): Future[Seq[JobDetails]] =
    getByFunctionId(id, limit, offset, Seq.empty)

  def update(jobDetails: JobDetails): Future[Unit]

  def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]

  def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]
  def getAll(limit: Int, offset: Int): Future[Seq[JobDetails]] = getAll(limit, offset, Seq.empty)

  def getByWorkerIdBeforeDate(workerId: String, timestamp: Long): Future[Seq[JobDetails]]

  def clear(): Future[Unit]

}

