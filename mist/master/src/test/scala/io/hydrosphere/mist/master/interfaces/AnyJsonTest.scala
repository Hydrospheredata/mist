package io.hydrosphere.mist.master.interfaces

import org.scalatest.FunSuite
import spray.json._

import scala.language.postfixOps

class AnyJsonTest extends FunSuite with DefaultJsonProtocol with AnyJsonFormat {

  test("AnyJsonFormat read") {
    assert(
      5 == AnyFormat.read(JsNumber(5)) &&
        "TestString" == AnyFormat.read(JsString("TestString")) &&
        Map.empty[String, JsValue] == AnyFormat.read(JsObject(Map.empty[String, JsValue])) &&
        true == AnyFormat.read(JsTrue) &&
        false == AnyFormat.read(JsFalse)
    )
  }

  test("AnyJsonFormat write") {
    assert(
      JsNumber(5) == AnyFormat.write(5) &&
        JsString("TestString") == AnyFormat.write("TestString") &&
        JsArray(JsNumber(1), JsNumber(1), JsNumber(2)) == AnyFormat.write(Seq(1, 1, 2)) &&
        JsObject(Map.empty[String, JsValue]) == AnyFormat.write(Map.empty[String, JsValue]) &&
        JsTrue == AnyFormat.write(true) &&
        JsFalse == AnyFormat.write(false)
    )
  }

  test("AnyJsonFormat serializationError") {
    intercept[spray.json.SerializationException] {
      val unknown = Set(1, 2)
      AnyFormat.write(unknown)
    }
  }

  test("AnyJsonFormat deserilalizationError") {
    intercept[spray.json.DeserializationException] {
      val unknown = JsNull
      AnyFormat.read(unknown)
    }
  }
}
