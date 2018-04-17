package mist.api.codecs

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
      val enc = implicitly[Encoder[TestPlain]]
      enc(TestPlain(99, "foo")) shouldBe
        JsLikeMap("a" -> JsLikeNumber(99), "b" -> JsLikeString("foo"))
    }

    it("should encode complex case class") {
      val enc = implicitly[Encoder[TestComplex]]

      val exp = JsLikeMap(
        "a" -> JsLikeNumber(99),
        "b" -> JsLikeList(Seq(
          JsLikeMap("a" -> JsLikeNumber(1), "b" -> JsLikeString("a")),
          JsLikeMap("a" -> JsLikeNumber(2), "b" -> JsLikeString("2"))
        ))
      )
      val value = TestComplex(99, Seq(TestPlain(1, "a"), TestPlain(2, "2")))
      enc(value) shouldBe exp
    }
  }
}
