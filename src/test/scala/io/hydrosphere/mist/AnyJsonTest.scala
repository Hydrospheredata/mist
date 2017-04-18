package io.hydrosphere.mist

import io.hydrosphere.mist.utils.json.AnyJsonFormatSupport
import org.scalatest.FunSuite
import spray.json._

import scala.language.postfixOps

class AnyJsonTest extends FunSuite with DefaultJsonProtocol with AnyJsonFormatSupport {

  test("AnyJsonFormat read") {
    assert(
      5 == AnyJsonFormat.read(JsNumber(5)) &&
        "TestString" == AnyJsonFormat.read(JsString("TestString")) &&
        Map.empty[String, JsValue] == AnyJsonFormat.read(JsObject(Map.empty[String, JsValue])) &&
        true == AnyJsonFormat.read(JsTrue) &&
        false == AnyJsonFormat.read(JsFalse)
    )
  }

  test("AnyJsonFormat write") {
    assert(
      JsNumber(5) == AnyJsonFormat.write(5) &&
        JsString("TestString") == AnyJsonFormat.write("TestString") &&
        JsArray(JsNumber(1), JsNumber(1), JsNumber(2)) == AnyJsonFormat.write(Seq(1, 1, 2)) &&
        JsObject(Map.empty[String, JsValue]) == AnyJsonFormat.write(Map.empty[String, JsValue]) &&
        JsTrue == AnyJsonFormat.write(true) &&
        JsFalse == AnyJsonFormat.write(false)
    )
  }

  test("AnyJsonFormat serializationError") {
    intercept[spray.json.SerializationException] {
      val unknown = Set(1, 2)
      AnyJsonFormat.write(unknown)
    }
  }

  test("AnyJsonFormat deserilalizationError") {
    intercept[spray.json.DeserializationException] {
      val unknown = JsNull
      AnyJsonFormat.read(unknown)
    }
  }
}
