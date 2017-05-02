package io.hydrosphere.mist.master.interfaces.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.hydrosphere.mist.master.MasterService
import org.scalatest.{FunSpec, Matchers}
import org.mockito.Mockito._
import org.mockito.Matchers._

class HttpApiV2Spec extends FunSpec with Matchers with ScalatestRouteTest {

  import JsonCodecs._

  it("should run job") {
    val master = mock(classOf[MasterService])
    val api = new HttpApiV2(master).route

    Post("/v2/api/jobs/x", Map("1" -> "Hello")) ~> api ~> check {
      status === StatusCodes.OK
    }
  }
}
