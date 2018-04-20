package mist.api.encoding

import mist.api.data._
import mist.api.data.JsSyntax._
import org.scalatest.{FunSpec, Matchers}
import shadedshapeless.LabelledGeneric

case class TestPlain(
  a: Int,
  b: String
)

case class TestComplex(
  a: Int,
  b: Seq[TestPlain]
)

class GenericInstancesSpec extends FunSpec with Matchers {

  import defaults._

  describe("generic instances") {
    it("should encode case class") {
      val enc = generic.encoder[TestPlain]
      enc(TestPlain(99, "foo")) shouldBe JsMap("a" -> 99.js, "b" -> "foo".js)
    }

    it("should encode complex case class") {
      implicit val inner: JsEncoder[TestPlain] = generic.encoder[TestPlain]
      val enc = generic.encoder[TestComplex]

      val exp = JsMap(
        "a" -> 99.js,
        "b" -> JsList(Seq(
          JsMap("a" -> 1.js, "b" -> "a".js),
          JsMap("a" -> 2.js, "b" -> "2".js)
        ))
      )
      val value = TestComplex(99, Seq(TestPlain(1, "a"), TestPlain(2, "2")))
      enc(value) shouldBe exp
    }
  }
}
