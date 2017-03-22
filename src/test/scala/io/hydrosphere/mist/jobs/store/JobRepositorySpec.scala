package io.hydrosphere.mist.jobs.store


import java.nio.file.{Files, Paths}

import io.hydrosphere.mist.jobs.{FullJobConfiguration, JobConfiguration, JobDetails}
import io.hydrosphere.mist.utils.TypeAlias._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpecLike, Matchers}

trait TargetRepository {
  val repo: JobRepository
}

trait JobRepositorySpec extends FlatSpecLike with Matchers with BeforeAndAfter {
  self: TargetRepository =>

  after {
    repo.clear()
  }

  it should "update" in {
    val details = fixtureJobDetails("id")
    repo.update(details)
    repo.get(details.jobId) shouldBe Some(details)
  }

  it should "remove" in {
    val details = fixtureJobDetails("id")
    repo.remove(details.jobId)
    repo.get(details.jobId) shouldBe None
  }

  it should "return correct size" in {
    (1 to 10).foreach(i => repo.update(fixtureJobDetails(s"jobId $i")))
    repo.size shouldBe 10
  }

  it should "clear" in {
    (1 to 10).foreach(i => repo.update(fixtureJobDetails(s"jobId $i")))
    repo.clear()
    repo.size shouldBe 0

  }

  it should "filter by status" in {
    (1 to 2).foreach(i => {
      val details = fixtureJobDetails(s"jobId $i", JobDetails.Status.Running)
      repo.update(details)
    })
    repo.update(fixtureJobDetails("ignore"))

    val runningJobs = repo.filteredByStatuses(List(JobDetails.Status.Running))

    runningJobs.size shouldBe 2
  }

  private def fixtureJobDetails(
    jobId: String,
    status: JobDetails.Status = JobDetails.Status.Initialized): JobDetails = {
    val conf = FullJobConfiguration(
      path = "path",
      className = "com.yoyo.MyClass",
      namespace = "namespace",
      parameters = JobParameters("key" -> "value"),
      externalId = Some("externalId"),
      route = Some("route"),
      action = JobConfiguration.Action.Serve
    )

    JobDetails(
      configuration = conf,
      source = JobDetails.Source.Cli,
      jobId = jobId,
      status = status
    )
  }

}

class InMemorySpec extends TargetRepository with JobRepositorySpec {
  override val repo: JobRepository = new InMemoryJobRepository
}

class MapDbSpec extends TargetRepository
  with JobRepositorySpec with BeforeAndAfterAll {

  val path = Paths.get("target", "map_db_repo")

  override def afterAll(): Unit = Files.delete(path)

  override val repo: JobRepository = new MapDbJobRepository(path.toString)
}

class SqliteSpec extends TargetRepository
  with JobRepositorySpec with BeforeAndAfterAll {

  val path = Paths.get("target", "sqlite_repo")

  override def afterAll(): Unit = Files.delete(path)

  override val repo: JobRepository = new SqliteJobRepository(path.toString)
}


