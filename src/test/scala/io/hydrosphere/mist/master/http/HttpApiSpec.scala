package io.hydrosphere.mist.master.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
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

}
