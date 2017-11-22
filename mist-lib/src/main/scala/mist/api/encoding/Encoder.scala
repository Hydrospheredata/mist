package mist.api.encoding

import mist.api.data.JsLikeData

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find Encoder instance for ${A}")
trait Encoder[A] extends Serializable {

  def apply(a : A): JsLikeData

}
