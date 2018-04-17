package mist.api.jdsl

import mist.api.data.{JsLikeData, JsLikeNull}
import mist.api.codecs.Encoder

/**
  *  Job result for java api
  */
trait RetVal {
  def encoded(): JsLikeData
}

object RetVal {
  def apply[T](value: T, encoder: Encoder[T]): RetVal = new RetVal {
    override def encoded(): JsLikeData = encoder(value)
  }
  def static(f: => JsLikeData): RetVal = new RetVal {
    override def encoded(): JsLikeData = f
  }
}

trait RetVals {

  def fromAny(t: Any): RetVal = RetVal(t, Encoder[Any](JsLikeData.fromJava))
  val empty: RetVal = RetVal.static(JsLikeNull)

}

object RetVals extends RetVals

