package io.hydrosphere.mist.master.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.jobs.runners.jar._
import io.hydrosphere.mist.jobs.{JvmJobInfo, JobDefinition, PyJobInfo}
import io.hydrosphere.mist.master.WorkerLink
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpApiSpec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  it("should serve jobs route") {
    val master = mock(classOf[MasterService])
    val api = new HttpApi(master).route

    val activeJobs = Map("job1" -> JobExecutionStatus())
    when(master.jobsStatuses()).thenReturn(activeJobs)

    Get("/internal/jobs") ~> api ~> check {
      status === StatusCodes.OK

      val r = responseAs[Map[String, JobExecutionStatus]]
      r shouldBe Map("job1" -> JobExecutionStatus())
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
    when(master.listRoutes()).thenReturn(Seq(pyInfo, jvmInfo))

    Get("/internal/routes") ~> api ~> check {
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

}
