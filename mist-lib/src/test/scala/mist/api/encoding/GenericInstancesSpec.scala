package mist.api.encoding

import mist.api.Extracted
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

case class TestBaseDefaults(
  a: Int,
  yoyo: String = "yoyo"
)

case class TestDefaults(
  a: Int,
  b: String = "DEFAULT",
  c: TestPlain = TestPlain(1, "yoyo"),
  d: Boolean
)

class GenericInstancesSpec extends FunSpec with Matchers {

  import defaults._

  describe("generic encoding") {

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

  describe("generic extraction") {

    it("should extract complex case class") {
      implicit val inner = generic.extractor[TestPlain]
      val ext = generic.extractor[TestDefaults]

      val in = JsMap(
        "a" -> 99.js,
        "d" -> false.js,
        "c" -> JsMap("a" -> 1.js, "b" -> "abc".js),
        "b" -> "B value".js
      )

      val exp = TestDefaults(99, "B value", TestPlain(1, "abc"), false)
      ext(in) shouldBe Extracted(exp)
    }

    it("should fallback onto defaults") {
      implicit val enc = generic.encoder[TestPlain]
      implicit val inner = generic.extractor[TestPlain]
      val ext = generic.extractorWithDefaults[TestDefaults]

      val in = JsMap(
        "a" -> 99.js,
        "d" -> false.js
      )
      val exp = TestDefaults(a = 99, d = false)
      ext(in) shouldBe Extracted(exp)
    }
  }
}
