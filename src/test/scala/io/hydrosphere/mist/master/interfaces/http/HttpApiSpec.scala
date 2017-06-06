package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.Messages.JobMessages.JobParams
import io.hydrosphere.mist.jobs.JobDetails.Source
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.jobs.jar._
import io.hydrosphere.mist.master.interfaces.JsonCodecs
import io.hydrosphere.mist.master.models.{JobStartRequest, RunSettings}
import io.hydrosphere.mist.master.{MasterService, WorkerLink}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpApiSpec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  val details = JobDetails(
    params = JobParams("path", "className", Map.empty, Action.Execute),
    jobId = "id",
    source = Source.Http,
    endpoint = "endpoint",
    context = "context",
    externalId = None
  )

  it("should serve active jobs") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.activeJobs()).thenReturn(
      Future.successful(
        List(details)
      )
    )

    Get("/internal/jobs") ~> api ~> check {
      status === StatusCodes.OK

      val r = responseAs[List[JobDetails]]
      r.size shouldBe 1
    }
  }

  it("should stop job") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.stopJob(any[String])).thenReturn(Future.successful(Some(details)))

    Delete("/internal/jobs/namespace/id") ~> api ~> check {
      status === StatusCodes.OK
    }
  }

  it("should serve workers") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.workers()).thenReturn(
      Future.successful(
        List(WorkerLink("name", "address"))
      ))

    Get("/internal/workers") ~> api ~> check {
      status === StatusCodes.OK

      val r = responseAs[List[WorkerLink]]
      r shouldBe List(WorkerLink("name", "address"))
    }
  }

  it("should stop worker") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.stopWorker(any[String])).thenReturn(Future.successful("id"))

    Delete("/internal/workers/id") ~> api ~> check {
      status === StatusCodes.OK
    }
  }

  it("should stop workers") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.stopAllWorkers()).thenReturn(Future.successful(()))

    Delete("/internal/workers") ~> api ~> check {
      status === StatusCodes.OK
    }
  }


  it("should serve routes") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    val pyInfo = PyJobInfo(
      JobDefinition("pyjob", "path_to_job.py", "PyJob", "namespace"))

    val testJobClass = io.hydrosphere.mist.jobs.jar.MultiplyJob
    val jvmInfo = JvmJobInfo(
      JobDefinition("scalajob", "path_to_jar.jar", "ScalaJob", "namespace"),
      JobsLoader.Common.loadJobClass(testJobClass.getClass.getCanonicalName).get
    )
    when(master.listEndpoints()).thenReturn(Seq(pyInfo, jvmInfo))

    Get("/internal/routers") ~> api ~> check {
      status === StatusCodes.OK

      responseAs[Map[String, HttpJobInfo]] should contain allOf(
        "pyjob" -> HttpJobInfo.forPython("pyjob"),
        "scalajob" -> HttpJobInfo(
          name = "scalajob",
          execute = Some(
            Map("numbers" -> HttpJobArg("MList", Seq(HttpJobArg("MInt", Seq.empty))))
          )
        )
      )
    }
  }

  it("should start job") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route
    when(master.forceJobRun(any[JobStartRequest], any[Source], any[Action]))
      .thenReturn(Future.successful(
        JobResult.success(
          Map("yoyo" -> "hello"),
          JobStartRequest("my-job", Map.empty, None, RunSettings.Default))
      ))

    Post("/api/my-job", Map("Hello" -> "123")) ~> api ~> check {
      status === StatusCodes.OK
    }
  }

}
