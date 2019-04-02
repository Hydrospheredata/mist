package io.hydrosphere.mist

import com.zaxxer.hikari.HikariConfig
import io.hydrosphere.mist.core.CommonData.{Action, JobParams}
import io.hydrosphere.mist.master.{DbConfig, JobDetails}
import io.hydrosphere.mist.master.JobDetails.Source
import io.hydrosphere.mist.master.store.{HikariDataSourceTransactor, HikariJobRepository, JobRepository, PgJobRequestSql}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}
import mist.api.data.JsMap

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class PgSpec extends FunSpec
  with BeforeAndAfterAll
  with Matchers
  with Eventually {
  
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(1, Seconds)))
  
  var pgContainer: TestContainer = _
  var repo: HikariJobRepository = _
  
  override def beforeAll = {
    pgContainer = TestContainer.run(DockerImage("postgres","latest"), Map(5433 -> 5432))
    
    val cfg = DbConfig.JDBCDbConfig(
      10,
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5433/postgres",
      Some("postgres"),
      Some("postgres"),
      true
    )
    
    repo = JobRepository.create(cfg) match {
      case Left(e) => throw e
      case Right(r) => r
    }
  }
  override def afterAll = {
    pgContainer.close()
    repo.shutdown()
  }
  
  private def await[A](f: Future[A]): A = Await.result(f, Duration.Inf)
  
  
  it("remove") {
    val details = fixtureJobDetails("id")
    await(repo.remove(details.jobId))
    await(repo.get(details.jobId)) shouldBe None
  }
  
  it("update") {
    val details = fixtureJobDetails("id")
    await(repo.update(details))
    await(repo.get(details.jobId)) shouldBe Some(details)
  }
  
  it("clear") {
    (1 to 10).foreach(i => await(repo.update(fixtureJobDetails(s"jobId $i"))))
    await(repo.clear())
    await(repo.getAll(10, 0)).size shouldBe 0
  }
  
  it("filter by status") {
    (1 to 2).foreach(i => {
      val details = fixtureJobDetails(s"jobId $i", JobDetails.Status.Started)
      await(repo.update(details))
    })
    await(repo.update(fixtureJobDetails("ignore")))
    
    val runningJobs = repo.filteredByStatuses(List(JobDetails.Status.Started))
    await(runningJobs).size shouldBe 2
  }
  
  it("decode failure") {
    val details = fixtureJobDetails("failed").withFailure("Test Error")
    await(repo.update(details))
    await(repo.get("failed")) shouldBe Some(details)
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

