package mist.api.jdsl

import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.encoding.JsEncoder

/**
  *  Job result for java api
  */
trait RetVal {
  def encoded(): JsLikeData
}

object RetVal {
  def apply[T](value: T, encoder: JsEncoder[T]): RetVal = new RetVal {
    override def encoded(): JsLikeData = encoder(value)
  }
  def static(f: => JsLikeData): RetVal = new RetVal {
    override def encoded(): JsLikeData = f
  }
}

trait RetVals {

  def fromAny(t: Any): RetVal = RetVal(t, JsEncoder[Any](JsLikeData.fromJava))
  val empty: RetVal = RetVal.static(JsLikeNull)

}

object RetVals extends RetVals

