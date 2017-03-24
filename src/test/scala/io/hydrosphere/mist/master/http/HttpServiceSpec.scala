package io.hydrosphere.mist.master.http

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport
import io.hydrosphere.mist.master.WorkerLink
import org.mockito.Mockito._
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Future

class HttpServiceSpec extends FunSpec with Matchers with ScalatestRouteTest {

  describe("ui") {

    it("should return index.html") {
      Get("/ui/") ~> HttpUi.route ~> check {
        status === StatusCodes.OK
        contentType == ContentTypes.`text/html(UTF-8)`
      }
    }

    it("should serve static resources") {
      Get("/ui/images/favicon.ico") ~> HttpUi.route ~> check {
        status === StatusCodes.OK
      }
      Get("/ui/NOT_FOUND") ~> HttpUi.route ~> check {
        status === StatusCodes.NotFound
      }
    }
  }

  describe("api") {

    import JsonCodecs._
    import io.circe.generic.auto._


    it("should serve jobs route") {
      val master = mock(classOf[ClusterMaster])
      val api = new HttpApi(master).route

      val activeJobs = Map("job1" -> JobExecutionStatus())
      when(master.jobsStatuses()).thenReturn(activeJobs)

      Get("/internal/jobs") ~> api ~> check {
        status === StatusCodes.OK

        val r = responseAs[Map[String, JobExecutionStatus]]
        r shouldBe Map("job1" -> JobExecutionStatus())
      }
    }

    it("should serve workers") {
      val master = mock(classOf[ClusterMaster])
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
}
