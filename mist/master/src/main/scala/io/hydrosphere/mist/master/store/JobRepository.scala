package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.JobDetails.Status

import scala.concurrent.{ExecutionContext, Future}

trait JobRepository {

  val activeStatuses = List(Status.Queued, Status.Started, Status.Initialized)

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
  def getByWorkerId(workerId: String): Future[Seq[JobDetails]]

  def clear(): Future[Unit]

  def running(): Future[Seq[JobDetails]] = filteredByStatuses(activeStatuses)

  def path(jobId: String)(f: JobDetails => JobDetails)(implicit ec: ExecutionContext): Future[Unit] = {
    get(jobId).flatMap {
      case Some(d) => update(f(d))
      case None => Future.failed(new IllegalStateException(s"Not found job: $jobId"))
    }
  }
}

