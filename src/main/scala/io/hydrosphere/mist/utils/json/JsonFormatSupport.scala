package io.hydrosphere.mist.utils.json

import spray.json.{DefaultJsonProtocol, JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat, deserializationError, serializationError}

import scala.language.reflectiveCalls

private[mist] trait JsonFormatSupport extends DefaultJsonProtocol {

  /** We must implement json parse/serializer for [[Any]] type */
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case number: Int => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case map: Map[String, _] => mapFormat[String, Any] write map
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case unknown: Any => serializationError("Do not understand object of type " + unknown.getClass.getName)
    }

    def read(value: JsValue): Any = value match {
      case JsNumber(number) => 
        try {
          number.toIntExact
        } catch {
          case _: ArithmeticException => number.toDouble
        }
      case JsString(string) => string
      case _: JsArray => listFormat[Any].read(value)
      case _: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown: Any => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }
}

