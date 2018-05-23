package mist.api.encoding

import mist.api.Extracted
import mist.api.data._
import JsSyntax._
import org.scalatest.{FunSpec, Matchers}

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

case class Inner(
  x: String,
  b: Int = 42
)
case class Root(
  a: Int,
  inner: Inner
)

case class WithOptions(
  a: Int,
  opt1: Option[String] = None,
  opt2: Option[Long] = Some(1L)
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

    it("should fallback onto defaults - inner case class") {
      implicit val enc = generic.encoder[Inner]
      implicit val innerExt = generic.extractorWithDefaults[Inner]
      val ext = generic.extractorWithDefaults[Root]

      val in = JsMap(
        "a" -> 54.js,
        "inner" -> JsMap(
          "x" -> "y".js
        )
      )
      ext(in) shouldBe Extracted(Root(a = 54, Inner(x = "y")))
    }

    it("defaults shouldn't ignore values") {
      implicit val enc = generic.encoder[Inner]
      implicit val innerExt = generic.extractorWithDefaults[Inner]
      val ext = generic.extractorWithDefaults[Root]

      val in = JsMap(
        "a" -> 54.js,
        "inner" -> JsMap(
          "x" -> "y".js,
          "b" -> -1.js
        )
      )
      ext(in) shouldBe Extracted(Root(a = 54, Inner(x = "y", b = -1)))
    }

    it("defaults should behave correctly with option fields") {
      val ext = generic.extractorWithDefaults[WithOptions]
      val in = JsMap("a" -> 42.js)
      ext(in) shouldBe Extracted(WithOptions(42))

      val in2 = JsMap(
        "a" -> 42.js,
        "opt1" -> "zz".js,
        "opt2" -> 2.js
      )
      ext(in2) shouldBe Extracted(WithOptions(42, Some("zz"), Some(2L)))
    }
  }
}
