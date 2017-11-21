package mist.api.jdsl

import mist.api.data.{JsLikeData, JsLikeUnit}
import mist.api.encoding.Encoder

/**
  *  Job result for java api
  */
case class RetVal[T](value: T, encoder: Encoder[T]) {
  def encoded(): JsLikeData = encoder(value)
}

trait RetVals {

  def fromAny[T](t: T): RetVal[T] = RetVal(t, new Encoder[T] {
    override def apply(a: T): JsLikeData = JsLikeData.fromJava(a)
  })

  def empty(): RetVal[Void] = RetVal(null, new Encoder[Void] {
    override def apply(a: Void): JsLikeData = JsLikeUnit
  })

}

object RetVals extends RetVals

