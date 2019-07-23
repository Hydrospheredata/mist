package io.hydrosphere.mist.master.store

import io.hydrosphere.mist.master.FilterClause.{ByFunctionId, ByStatuses, ByWorkerId}
import io.hydrosphere.mist.master.JobDetails.Status._
import io.hydrosphere.mist.master.{JobDetails, JobDetailsRequest}
import org.scalatest.{FunSpec, Matchers}
import doobie.implicits._
import io.hydrosphere.mist.core.CommonData.{Action, JobParams}
import io.hydrosphere.mist.master.JobDetails.Source
import mist.api.data.JsMap
import cats.data.NonEmptyList

class JobRequestsSqlSpec extends FunSpec with Matchers{

  describe("h2 sql") {
    test(new H2JobRequestSql)
  }
  
  describe("postgres sql") {
    test(new PgJobRequestSql)
  }
  
  def test(jobRequestSql: JobRequestSql): Unit = {
    it("select(*)") {
      val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0))
      sql.toString() shouldEqual sql"select * from job_details order by create_time desc limit ? offset ? ".toString()
    }

    it("filter by function") {
      val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).withFilter(ByFunctionId("ID")))
      sql.toString() shouldEqual sql"select * from job_details where function = ? order by create_time desc limit ? offset ? ".toString()
    }

    it("filter by worker") {
      val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).withFilter(ByWorkerId("ID")))
      sql.toString() shouldEqual sql"select * from job_details where worker_id = ? order by create_time desc limit ? offset ? ".toString()
    }

    it("sql filter by status") {
      val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).withFilter(ByStatuses(NonEmptyList.of(Initialized, Queued))))
      sql.toString() shouldEqual sql"select * from job_details where status IN (?, ?) order by create_time desc limit ? offset ? ".toString()
    }

    it("sql complex filter") {
      val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).
        withFilter(ByStatuses(NonEmptyList.of(Initialized, Queued))).
        withFilter(ByWorkerId("ID")).
        withFilter(ByFunctionId("ID")))
      sql.toString() shouldEqual sql"select * from job_details where function = ? and worker_id = ? and status IN (?, ?) order by create_time desc limit ? offset ? ".toString()
    }
  }
  
  private def fixtureJobDetails(
    jobId: String,
    status: JobDetails.Status = JobDetails.Status.Initialized): JobDetails = {
    JobDetails(
      params = JobParams("path", "className", JsMap.empty, Action.Execute),
      jobId = jobId,
      source = Source.Http,
      function = "function",
      context = "context",
      externalId = None,
      status = status
    )
  }
  
}
