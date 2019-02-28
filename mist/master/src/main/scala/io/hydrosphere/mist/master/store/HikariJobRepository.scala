package io.hydrosphere.mist.master.store

import cats.data.NonEmptyList
import io.hydrosphere.mist.master.{JobDetails, JobDetailsRecord, JobDetailsRequest, JobDetailsResponse}
import doobie._
import doobie.implicits._
import io.hydrosphere.mist.master.FilterClause.{ByFunctionId, ByStatuses, ByWorkerId}

import scala.concurrent.Future

class HikariJobRepository(hikari: HikariDataSourceTransactor) extends JobRepository {

  val beginSql = sql"select * from job_details "

  override def remove(jobId: String): Future[Unit] = {
    sql"delete from job_details where job_id = $jobId".
      update.
      run.
      transact(hikari.transactor).
      map(_ => {}).
      unsafeToFuture()
  }

  override def get(jobId: String): Future[Option[JobDetails]] = {
    (beginSql ++ sql" where job_id = $jobId").
      query[JobDetailsRecord].
      map(_.toJobDetails).
      option.
      transact(hikari.transactor).
      unsafeToFuture()
  }

  /**
    * This method uses different syntax for the different databases while MERGE
    * syntax is not supported by PostgreSQL
    */
  override def update(jobDetails: JobDetails): Future[Unit] = {
    var r: JobDetailsRecord = JobDetailsRecord(jobDetails)

    var updateSQL = if (hikari.ds.getJdbcUrl.startsWith("jdbc:h2")) {
      sql"""
          merge into job_details
          (
            path, class_name, namespace, parameters,
            external_id, function, action, source,
            job_id, start_time, end_time, job_result,
            status, worker_id, create_time
          ) key(job_id)
          values
          (
            ${r.path}, ${r.className}, ${r.namespace}, ${r.parameters},
            ${r.externalId}, ${r.function}, ${r.action}, ${r.source},
            ${r.jobId}, ${r.startTime}, ${r.endTime}, ${r.jobResult},
            ${r.status}, ${r.workerId}, ${r.createTime}
          )"""
    } else if (hikari.ds.getJdbcUrl.startsWith("jdbc:postgresql")) {
      sql"""
            insert into job_details
            (
              path, class_name, namespace, parameters,
              external_id, function, action, source,
              job_id, start_time, end_time, job_result,
              status, worker_id, create_time
            ) values
            (
              ${r.path}, ${r.className}, ${r.namespace}, ${r.parameters},
              ${r.externalId}, ${r.function}, ${r.action}, ${r.source},
              ${r.jobId}, ${r.startTime}, ${r.endTime}, ${r.jobResult},
              ${r.status}, ${r.workerId}, ${r.createTime}
            ) on conflict (job_id) do update set
              path = ${r.path},
              class_name = ${r.className},
              namespace = ${r.namespace},
              parameters = ${r.parameters},
              external_id = ${r.externalId},
              function = ${r.function},
              action = ${r.action},
              source = ${r.source},
              start_time = ${r.startTime},
              end_time = ${r.endTime},
              job_result = ${r.jobResult},
              status = ${r.status},
              worker_id = ${r.workerId},
              create_time = ${r.createTime}
        """
    } else {
      throw new RuntimeException("Update method is intended only for H2 and PostgreSQL connections!");
    }
    updateSQL.
      update.
      run.
      transact(hikari.transactor).
      map(_ => {}).
      unsafeToFuture()
  }

  protected def jobDetailStatusToString(seq: Seq[JobDetails.Status]): NonEmptyList[String] = {
    NonEmptyList.fromList(seq.map(_.toString).toList).get
  }

  protected def sqlToJobDetailsSeq(sql: Fragment): Future[Seq[JobDetails]] = {
    sql.
      query[JobDetailsRecord].
      map(_.toJobDetails).
      to[Seq].
      transact(hikari.transactor).
      unsafeToFuture()
  }

  override def filteredByStatuses(statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {
    val allSQL = if (statuses != null && statuses.nonEmpty) {
      beginSql ++ sql"where " ++ Fragments.in(fr"status", jobDetailStatusToString(statuses))
    } else {
      beginSql
    }
    sqlToJobDetailsSeq(allSQL)
  }

  override def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Future[Seq[JobDetails]] = {

    val endOfSQL = sql" limit $limit offset $offset"

    val allSQL = if (statuses != null && statuses.nonEmpty) {
      beginSql ++ sql"where " ++ Fragments.in(fr"status", jobDetailStatusToString(statuses)) ++ endOfSQL
    } else {
      beginSql ++ endOfSQL
    }
    sqlToJobDetailsSeq(allSQL)
  }

  override def clear(): Future[Unit] = {
    sql"truncate table job_details".
      update.
      run.
      transact(hikari.transactor).
      map(_ => {}).
      unsafeToFuture()
  }

  override def getJobs(req: JobDetailsRequest): Future[JobDetailsResponse] = {
    generateSqlByJobDetailsRequest(req).
      query[JobDetailsRecord].
      map(_.toJobDetails).
      to[Seq].
      map(seq => JobDetailsResponse(seq, seq.size)).
      transact(hikari.transactor).
      unsafeToFuture()
  }

  /**
    * Generates Doobie sql fragment with JobDetailsRequest
    */
  def generateSqlByJobDetailsRequest(req: JobDetailsRequest): Fragment = {
    val filters =
      if (req.filters.isEmpty) sql"" else {
        fr"where" ++ req.filters.map(f => f match {
          case ByFunctionId(id) => fr"function = $id"
          case ByStatuses(statuses) => Fragments.in(fr"status", jobDetailStatusToString(statuses))
          case ByWorkerId(id) => fr"worker_id = $id"
        }).reduce((a, b) => a ++ fr"and" ++ b)
      }
    val limit = fr"limit ${req.limit} offset ${req.offset}"
    val order = fr"order by create_time desc"
    beginSql ++ filters ++ order ++ limit
  }
}
