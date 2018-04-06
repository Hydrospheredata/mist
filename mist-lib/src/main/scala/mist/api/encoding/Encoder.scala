package mist.api.encoding

import mist.api.data.JsLikeData

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find Encoder instance for ${A}")
trait Encoder[A] extends Serializable {

  def apply(a : A): JsLikeData

}

object Encoder {
  def apply[A](f: A => JsLikeData): Encoder[A] = new Encoder[A] {
    override def apply(a: A): JsLikeData = f(a)
  }
}
