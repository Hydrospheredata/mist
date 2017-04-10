package io.hydrosphere.mist.master.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.jobs.runners.jar._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.{MasterService, WorkerLink}
import io.hydrosphere.mist.utils.TypeAlias.JobParameters
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpApiSpec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  it("should serve active jobs") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.activeJobs()).thenReturn(
      List(
        JobDetails(
          JobExecutionParams("path", "MyClass", "namespace", JobParameters.empty, None, None),
          JobDetails.Source.Http
        )
      )
    )

    Get("/internal/jobs") ~> api ~> check {
      status === StatusCodes.OK

      val r = responseAs[Map[String, JobExecutionStatus]]
      r.values should contain (JobExecutionStatus())
    }
  }

  it("should stop job") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.stopJob(any[String])).
      thenReturn(Future.successful(()))

    Delete("/internal/jobs/id") ~> api ~> check {
      status === StatusCodes.OK
    }
  }

  it("should serve workers") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.workers()).thenReturn(
      Future.successful(
        List(
          WorkerLink("uid", "name", "address", blackSpot = false)
      )))

    Get("/internal/workers") ~> api ~> check {
      status === StatusCodes.OK

      val r = responseAs[List[WorkerLink]]
      r shouldBe List(WorkerLink("uid", "name", "address", blackSpot = false))
    }
  }

  it("should stop worker") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    when(master.stopWorker(any[String])).thenReturn(Future.successful(()))

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

    val testJobClass = io.hydrosphere.mist.jobs.runners.jar.MultiplyJob
    val jvmInfo = JvmJobInfo(
      JobDefinition("scalajob", "path_to_jar.jar", "ScalaJob", "namespace"),
      JobsLoader.Common.loadJobClass(testJobClass.getClass.getCanonicalName).get
    )
    when(master.listRoutesInfo()).thenReturn(Seq(pyInfo, jvmInfo))

    Get("/internal/routers") ~> api ~> check {
      status === StatusCodes.OK

      responseAs[Map[String, HttpJobInfo]] should contain allOf(
        "pyjob" -> HttpJobInfo.forPython(),
        "scalajob" -> HttpJobInfo(
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
    when(master.startJob(any[String], any[Action], any[JobParameters]))
      .thenReturn(Future.successful(
        JobResult.success(Map("yoyo" -> "hello"),
        JobExecutionParams("", "", "", Map.empty, None, None, Action.Execute))
      ))

    Post("/api/my-job", Map("Hello" -> "123")) ~> api ~> check {
      status === StatusCodes.OK
    }
  }

}
