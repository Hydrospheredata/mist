package io.hydrosphere.mist.master.store

import cats.data.NonEmptyList
import doobie.{Fragment, Fragments}
import doobie.implicits._
import io.hydrosphere.mist.master.FilterClause.{ByFunctionId, ByStatuses, ByWorkerId}
import io.hydrosphere.mist.master.{JobDetails, JobDetailsRecord, JobDetailsRequest}

class H2JobRequestSql extends JobRequestSql {
  override def update(jobDetails: JobDetails): Fragment = {
    val r: JobDetailsRecord = JobDetailsRecord(jobDetails)
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
  }
}

class PgJobRequestSql extends JobRequestSql {
  override def update(jobDetails: JobDetails): Fragment = {
    val r: JobDetailsRecord = JobDetailsRecord(jobDetails)
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
  }

}

object JobRequestSql {
  def apply(jdbcUrl: String): JobRequestSql = {
    jdbcUrl match {
      case s if s.startsWith("jdbc:h2") => new H2JobRequestSql()
      case s if s.startsWith("jdbc:postgresql") => new PgJobRequestSql()
      case _ =>
        throw new RuntimeException(s"Can't work with jdbc $jdbcUrl. Supported databses are PostgreSQL and H2");
    }
  }
}

trait JobRequestSql {

//  def flyDbMigrationPath: String

  /**
    * This method is abstract and uses different syntax for the different databases
    * while MERGE syntax is not supported by PostgreSQL
    */
  def update(jobDetails: JobDetails): Fragment

  def beginSql = sql"select * from job_details "

  def remove(jobId: String) = sql"delete from job_details where job_id = $jobId"

  def get(jobId: String): Fragment = beginSql ++ sql" where job_id = $jobId"

  def clear = sql"truncate table job_details"

  def filteredByStatuses(statuses: Seq[JobDetails.Status]): Fragment = {
    beginSql ++ statusFr(fr"where ", statuses)
  }

  def getAll(limit: Int, offset: Int, statuses: Seq[JobDetails.Status]): Fragment = {
    val endOfSQL = sql" limit $limit offset $offset"

    beginSql ++ statusFr(fr"where ", statuses) ++ endOfSQL
  }

  def statusFr(prefix: Fragment, statuses: Seq[JobDetails.Status]): Fragment = {
    NonEmptyList.fromList(statuses.toList) match {
      case None => Fragment.empty
      case Some(inSt) => prefix ++ Fragments.in(fr"status", inSt.map(_.toString))
    }
  }  

  /**
    * Generates Doobie sql fragment with JobDetailsRequest
    */
  def generateSqlByJobDetailsRequest(req: JobDetailsRequest): Fragment = {
    val filters =
      if (req.filters.isEmpty) sql"" else {
        fr"where" ++ req.filters.map {
          case ByFunctionId(id) => fr"function = $id"
          case ByStatuses(statuses) => Fragments.in(fr"status", statuses.map(_.toString))
          case ByWorkerId(id) => fr"worker_id = $id"
        }.reduce((a, b) => a ++ fr"and" ++ b)
      }
    val limit = fr"limit ${req.limit} offset ${req.offset}"
    val order = fr"order by create_time desc"
    beginSql ++ filters ++ order ++ limit
  }
}
