package io.hydrosphere.mist.master.interfaces

import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._
import spray.json._

import scala.language.postfixOps

class AnyJsonTest extends FunSpec with DefaultJsonProtocol with AnyJsonFormat with Matchers {

  val expected = Table[JsValue, Any](
    ("in", "out"),
    (JsNumber(5), 5),
    (JsNumber(5.5), 5.5),
    (JsNumber(12147483647L), 12147483647L),
    (JsString("str"), "str"),
    (JsTrue, true),
    (JsFalse, false),
    (JsArray(JsString("str"), JsTrue, JsNumber(42)), Seq("str", true, 42)),
    (JsObject("a" -> JsString("b")), Map("a" -> "b"))
  )

  it("should read") {
    forAll(expected) { (in, out) =>
      AnyFormat.read(in) shouldBe out
    }
  }
  it("should write") {
    forAll(expected) { (in, out) =>
      AnyFormat.write(out) shouldBe in
    }
  }

}
