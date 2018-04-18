package mist.api

import mist.api.args._
import mist.api.data.{JsLikeData, JsLikeMap}
import mist.api.encoding.JsEncoder

import scala.util.Try

trait LowHandle[+A] { self =>

  def invoke(ctx: FnContext): Try[A]
  def describe(): Seq[ArgInfo]
  def validate(params: JsLikeMap): Option[Throwable]

}

trait Handle extends LowHandle[JsLikeData]

object Handle {

  def fromLow[A](low: LowHandle[A], enc: JsEncoder[A]): Handle = {
    new Handle {
      override def invoke(ctx: FnContext): Try[JsLikeData] = low.invoke(ctx).map(enc.apply)
      override def describe(): Seq[ArgInfo] = low.describe()
      override def validate(params: JsLikeMap): Option[Throwable] = low.validate(params)
    }
  }
}
