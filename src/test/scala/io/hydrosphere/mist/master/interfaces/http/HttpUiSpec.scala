package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import org.scalatest.FunSpec

class HttpUiSpec extends FunSpec with ScalatestRouteTest {

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
