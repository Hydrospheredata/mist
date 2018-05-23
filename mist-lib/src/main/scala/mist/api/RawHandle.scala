package mist.api

import mist.api.data.{JsData, JsMap}
import mist.api.encoding.JsEncoder

import scala.util.Try

trait RawHandle[A] { self =>

  def invoke(ctx: FnContext): Try[A]
  def describe(): Seq[ArgInfo]
  def validate(params: JsMap): Extraction[Unit]
  def toHandle(enc: JsEncoder[A]): Handle = Handle.fromRaw(self, enc)

}

trait Handle extends RawHandle[JsData]

object Handle {

  def fromRaw[A](low: RawHandle[A], enc: JsEncoder[A]): Handle = {
    new Handle {
      override def invoke(ctx: FnContext): Try[JsData] = low.invoke(ctx).map(enc.apply)
      override def describe(): Seq[ArgInfo] = low.describe()
      override def validate(params: JsMap): Extraction[Unit] = low.validate(params)
    }
  }
}
