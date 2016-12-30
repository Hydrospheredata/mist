package io.hydrosphere.mist.utils.json

import java.util

import spray.json.{DefaultJsonProtocol, JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat, deserializationError, serializationError}

import scala.language.reflectiveCalls
import scala.collection.JavaConversions._

private[mist] trait AnyJsonFormatSupport extends DefaultJsonProtocol {

  /** We must implement json parse/serializer for [[Any]] type */
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any): JsValue = x match {
      case number: Int => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case javaList: util.ArrayList[_] => seqFormat[Any].write(javaList.toList)
      case array: Array[_] => seqFormat[Any].write(array.toList)
      case map: Map[String, _] => mapFormat[String, Any] write map
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case unknown: Any => serializationError("Do not understand object of type " + unknown.getClass.getCanonicalName)
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

