package io.hydrosphere.mist.master.store

import java.nio.file.Paths

import io.hydrosphere.mist.core.CommonData.{Action, JobParams}
import io.hydrosphere.mist.master.JobDetails
import io.hydrosphere.mist.master.JobDetails.Source
import org.scalatest._

class H2RepoSpec extends FlatSpec with Matchers with BeforeAndAfter with BeforeAndAfterAll {

  val path = Paths.get("./target", "h2_repo")

  override def afterAll(): Unit = org.h2.tools.DeleteDbFiles.execute("./target", "h2_repo", true)

  val repo: JobRepository = H2JobsRepository(path.toString)

  import io.hydrosphere.mist.master.TestUtils._

  after {
    repo.clear().await
  }

  it should "update" in {
    val details = fixtureJobDetails("id")
    repo.update(details).await
    repo.get(details.jobId).await shouldBe Some(details)
  }

  it should "remove" in {
    val details = fixtureJobDetails("id")
    repo.remove(details.jobId)
    repo.get(details.jobId).await shouldBe None
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

  private def fixtureJobDetails(
    jobId: String,
    status: JobDetails.Status = JobDetails.Status.Initialized): JobDetails = {
    JobDetails(
      params = JobParams("path", "className", Map.empty, Action.Execute),
      jobId = jobId,
      source = Source.Http,
      function = "function",
      context = "context",
      externalId = None,
      status = status,
      workerId = "workerId"
    )
  }

}


