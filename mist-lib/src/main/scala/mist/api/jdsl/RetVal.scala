package mist.api.jdsl

import mist.api.data.{JsData, JsNull}
import mist.api.encoding.JsEncoder

/**
  *  Job result for java api
  */
abstract class RetVal {
  def encoded(): JsData
}

object RetVal {

  def apply[T](value: T, encoder: JsEncoder[T]): RetVal = new RetVal {
    override def encoded(): JsData = encoder(value)
  }

  def fromJs(js: JsData): RetVal = new RetVal {
    override def encoded(): JsData = js
  }
}

//object RetVals extends {
//
//  def fromAny(t: Any): RetVal = RetVal(t, JsEncoder[Any](JsData.fromJava))
//  val empty: RetVal = RetVal.static(JsNull)
//
//}

