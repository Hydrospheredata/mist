package io.hydrosphere.mist.master.store

import com.zaxxer.hikari.{HikariConfig, HikariJNDIFactory}
import io.hydrosphere.mist.core.CommonData.{Action, JobParams}
import io.hydrosphere.mist.master.{JobDetails, JobDetailsRequest}
import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.TestUtils._
import mist.api.data.JsMap
import org.scalatest._
import doobie.implicits._
import io.hydrosphere.mist.master.FilterClause.{ByFunctionId, ByStatuses, ByWorkerId}
import io.hydrosphere.mist.master.JobDetails.Status.{Initialized, Queued}

abstract class HikariRepoSpec(className: String, jdbcUrl: String,
                              username: String, password: String)
  extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  val hikariConfig = new HikariConfig()
  hikariConfig.setDriverClassName(className)
  hikariConfig.setJdbcUrl(jdbcUrl)
  hikariConfig.setUsername(username)
  hikariConfig.setPassword(password)

  val hikari = new HikariDataSourceTransactor(hikariConfig)

  override def afterAll() {
    repo.clear().await
    hikari.shutdown()
  }

  /**
    * We want to test some internal function so we expose HikariJobRepository
    * but not super interface.
    */
  val repo: HikariJobRepository = new HikariJobRepository(hikari)
  val jobRequestSql: JobRequestSql = repo.jobRequestSql


  hikari.ds.getDriverClassName should "remove" in {
    val details = fixtureJobDetails("id")
    repo.remove(details.jobId)
    repo.get(details.jobId).await shouldBe None
  }

  it should "update" in {
    val details = fixtureJobDetails("id")
    repo.update(details).await
    repo.get(details.jobId).await shouldBe Some(details)
  }

  it should "clear" in {
    (1 to 10).foreach(i => repo.update(fixtureJobDetails(s"jobId $i")).await)
    repo.clear().await
    repo.getAll(10, 0).await.size shouldBe 0
  }

  it should "filter by status" in {
    (1 to 2).foreach(i => {
      val details = fixtureJobDetails(s"jobId $i", JobDetails.Status.Started)
      repo.update(details).await
    })
    repo.update(fixtureJobDetails("ignore")).await

    val runningJobs = repo.filteredByStatuses(List(JobDetails.Status.Started))
    runningJobs.await.size shouldBe 2
  }

  it should "decode failure" in {
    val details = fixtureJobDetails("failed").withFailure("Test Error")
    repo.update(details).await
    repo.get("failed").await shouldBe Some(details)
  }

  // Doobie specific test here

  it should "sql select(*)" in {
    val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0))
    sql.toString() shouldEqual sql"select * from job_details order by create_time desc limit ? offset ? ".toString()
  }

  it should "sql filter by function" in {
    val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).withFilter(ByFunctionId("ID")))
    sql.toString() shouldEqual sql"select * from job_details where function = ? order by create_time desc limit ? offset ? ".toString()
  }

  it should "sql filter by worker" in {
    val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).withFilter(ByWorkerId("ID")))
    sql.toString() shouldEqual sql"select * from job_details where worker_id = ? order by create_time desc limit ? offset ? ".toString()
  }

  it should "sql filter by status" in {
    val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).withFilter(ByStatuses(Seq(Initialized, Queued))))
    sql.toString() shouldEqual sql"select * from job_details where status IN (?, ?) order by create_time desc limit ? offset ? ".toString()
  }

  it should "sql complex filter" in {
    val sql = jobRequestSql.generateSqlByJobDetailsRequest(JobDetailsRequest(0, 0).
      withFilter(ByStatuses(Seq(Initialized, Queued))).
      withFilter(ByWorkerId("ID")).
      withFilter(ByFunctionId("ID")))
    sql.toString() shouldEqual sql"select * from job_details where function = ? and worker_id = ? and status IN (?, ?) order by create_time desc limit ? offset ? ".toString()
  }

  // Helper functions
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
