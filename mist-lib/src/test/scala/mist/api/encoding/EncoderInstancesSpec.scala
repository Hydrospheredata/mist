package mist.api.encoding

import mist.api.data._
import org.scalatest.{FunSpec, Matchers}

case class TestPlain(
  a: Int,
  b: String
)

case class TestComplex(
  a: Int,
  b: Seq[TestPlain]
)

class EncoderInstancesSpec extends FunSpec with Matchers {

  describe("generic instances") {
    import DefaultEncoderInstances._
    import GenericEncoderInstances._

    it("should encode case class") {
      val enc = implicitly[JsEncoder[TestPlain]]
      enc(TestPlain(99, "foo")) shouldBe
        JsLikeMap("a" -> JsNumber(99), "b" -> JsString("foo"))
    }

    it("should encode complex case class") {
      val enc = implicitly[JsEncoder[TestComplex]]

      val exp = JsLikeMap(
        "a" -> JsNumber(99),
        "b" -> JsList(Seq(
          JsLikeMap("a" -> JsNumber(1), "b" -> JsString("a")),
          JsLikeMap("a" -> JsNumber(2), "b" -> JsString("2"))
        ))
      )
      val value = TestComplex(99, Seq(TestPlain(1, "a"), TestPlain(2, "2")))
      enc(value) shouldBe exp
    }
  }
}
