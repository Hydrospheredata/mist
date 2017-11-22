package mist.api.encoding

import mist.api.data._

trait PrimitiveEncoders {

  implicit object ShortEncoder extends Encoder[Short] {
    def apply(d: Short): JsLikeData = JsLikeNumber(d.toInt)
  }

  implicit object IntEncoder extends Encoder[Int] {
    def apply(n: Int): JsLikeData = JsLikeNumber(n)
  }

  implicit object LongEncoder extends Encoder[Long] {
    def apply(d: Long): JsLikeData = JsLikeNumber(d)
  }

  implicit object FloatEncoder extends Encoder[Float] {
    def apply(n: Float): JsLikeData = JsLikeNumber(n.toDouble)
  }

  implicit object DoubleEncoder extends Encoder[Double] {
    def apply(n: Double): JsLikeData = JsLikeNumber(n)
  }

  implicit object StringEncoder extends Encoder[String] {
    def apply(a: String): JsLikeData = JsLikeString(a)
  }

  implicit object BooleanEncoder extends Encoder[Boolean] {
    def apply(b: Boolean): JsLikeData = JsLikeBoolean(b)
  }

  implicit object UnitEncoder extends Encoder[Unit] {
    def apply(u: Unit): JsLikeData = JsLikeUnit
  }
}
