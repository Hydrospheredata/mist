package mist.api.data

import org.scalatest.{FunSpec, Matchers}
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._

import scala.util.Success

class Json4sConversionSpec extends FunSpec with Matchers {

  it("should parse root") {
    val s =
      """{
        |  "a": 2147483648,
        |  "b": false,
        |  "c": "string",
        |  "d": 1.02,
        |  "e": ["1", false],
        |  "g": {
        |    "x": "y"
        |  }
        |}
      """.stripMargin

    val exp = JsMap(
      "a" -> 2147483648L.js,
      "b" -> false.js,
      "c" -> "string".js,
      "d" -> 1.02.js,
      "e" -> JsList(Seq("1".js, false.js)),
      "g" -> JsMap(
        "x" -> "y".js
      )
    )
    Json4sConversion.parseRoot(s) shouldBe Success(exp)
  }

  //TODO: jsMap doesn't preserve order
  it("should pretty print") {
    val exp = JsMap(
      "a" -> 2147483648L.js,
      "b" -> false.js,
      "c" -> "string".js,
      "d" -> 1.02.js,
      "e" -> JsList(Seq("1".js, false.js)),
      "g" -> JsMap(
        "x" -> "y".js
      )
    )
    Json4sConversion.formattedString(exp)
  }
}
