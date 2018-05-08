package mist.api.jdsl

import mist.api.data.{JsData, JsNull}
import mist.api.encoding.JsEncoder

/**
  *  Job result for java api
  */
trait RetVal {
  def encoded(): JsData
}

object RetVal {
  def apply[T](value: T, encoder: JsEncoder[T]): RetVal = new RetVal {
    override def encoded(): JsData = encoder(value)
  }
  def static(f: => JsData): RetVal = new RetVal {
    override def encoded(): JsData = f
  }
}

trait RetVals {

  def fromAny(t: Any): RetVal = RetVal(t, JsEncoder[Any](JsData.fromJava))
  val empty: RetVal = RetVal.static(JsNull)

}

object RetVals extends RetVals

