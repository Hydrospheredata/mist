package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.master.{JobDetails, JobDetailsRecord, JobDetailsRequest, JobDetailsResponse}
import doobie.implicits._

import scala.concurrent.Future

class HikariJobRepository(
  hikari: HikariDataSourceTransactor,
  jobRequestSql: JobRequestSql
) extends JobRepository {

  override def remove(jobId: String): Future[Unit] = {
    jobRequestSql.remove(jobId)
      .update
      .run
      .transact(hikari.transactor)
      .map(_ => {})
      .unsafeToFuture()
  }

  override def get(jobId: String): Future[Option[JobDetails]] = {
    jobRequestSql.get(jobId)
      .query[JobDetailsRecord]
      .map(_.toJobDetails)
      .option
      .transact(hikari.transactor)
      .unsafeToFuture()
  }

  override def update(jobDetails: JobDetails): Future[Unit] = {
    jobRequestSql.update(jobDetails)
      .update
      .run
      .transact(hikari.transactor)
      .map(_ => {})
      .unsafeToFuture()
  }

  override def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    jobRequestSql.filteredByStatuses(statuses)
      .query[JobDetailsRecord]
      .map(_.toJobDetails)
      .to[Seq]
      .transact(hikari.transactor)
      .unsafeToFuture()
  }

  override def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    jobRequestSql.getAll(limit, offset, statuses)
      .query[JobDetailsRecord]
      .map(_.toJobDetails)
      .to[Seq]
      .transact(hikari.transactor)
      .unsafeToFuture()
  }

  override def clear(): Future[Unit] = {
    jobRequestSql.clear
      .update
      .run
      .transact(hikari.transactor)
      .map(_ => {})
      .unsafeToFuture()
  }

  override def getJobs(req: JobDetailsRequest): Future[JobDetailsResponse] = {
    jobRequestSql.generateSqlByJobDetailsRequest(req)
      .query[JobDetailsRecord]
      .map(_.toJobDetails)
      .to[Seq]
      .map(seq => JobDetailsResponse(seq, seq.size))
      .transact(hikari.transactor)
      .unsafeToFuture()
  }
  
  def shutdown(): Unit = hikari.shutdown()
}
