package mist.api.jdsl

import mist.api.data.{JsData, JsNull, JsUnit}
import mist.api.encoding.JsEncoder

/**
  *  Job result for java api
  */
abstract class RetVal[T] {
  def value(): T
  def encoded(): JsData
}

object RetVal {

  def apply[T](v: T, encoder: JsEncoder[T]): RetVal[T] = new RetVal[T] {
    override def value(): T = v
    override def encoded(): JsData = encoder(v)
  }

  def fromJs(js: JsData): RetVal[JsData] = new RetVal[JsData] {
    override def value(): JsData = js
    override def encoded(): JsData = js
  }

  def fromAny(t: Any): RetVal[Any] = RetVal(t, JsEncoder[Any](JsData.fromJava))

  val empty: RetVal[Void] = new RetVal[Void] {
    override def value(): Void = null
    override def encoded(): JsData = JsUnit
  }

}
