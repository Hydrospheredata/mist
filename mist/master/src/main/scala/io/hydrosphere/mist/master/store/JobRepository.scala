package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.master.{JobDetails, JobDetailsRequest, JobDetailsResponse}
import io.hydrosphere.mist.master.JobDetails.Status

import scala.concurrent.{ExecutionContext, Future}

trait JobRepository {

  def remove(jobId: String): Future[Unit]

  def get(jobId: String): Future[Option[JobDetails]]

  def update(jobDetails: JobDetails): Future[Unit]

  def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]

  def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]]
  def getAll(limit: Int, offset: Int): Future[Seq[JobDetails]] = getAll(limit, offset, Seq.empty)

  def clear(): Future[Unit]

  def running(): Future[Seq[JobDetails]] = filteredByStatuses(JobDetails.Status.inProgress)

  def path(jobId: String)(f: JobDetails => JobDetails)(implicit ec: ExecutionContext): Future[Unit] = {
    get(jobId).flatMap {
      case Some(d) => update(f(d))
      case None => Future.failed(new IllegalStateException(s"Not found job: $jobId"))
    }
  }

  def getJobs(req: JobDetailsRequest): Future[JobDetailsResponse]
}

