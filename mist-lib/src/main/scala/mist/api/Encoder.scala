package mist.api

import mist.api.data._

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find Encoder instance for ${A}")
trait Encoder[A] extends Serializable {

  def apply(a : A): JsLikeData

}

trait PrimitiveEncoders {

  implicit class EncoderSyntax[A](val a: A)(implicit enc: Encoder[A]) {
    def encode(): Any = enc(a)
  }

  implicit object IntEncoder extends Encoder[Int] {
    def apply(a: Int): JsLikeData = JsLikeInt(a)
  }

  implicit object StringEncoder extends Encoder[String] {
    def apply(a: String): JsLikeData = JsLikeString(a)
  }

  implicit object BooleanEncoder extends Encoder[Boolean] {
    def apply(b: Boolean): JsLikeData = JsLikeBoolean(b)
  }

  implicit object DoubleEncoder extends Encoder[Double] {
    def apply(d: Double): JsLikeData = JsLikeDouble(d)
  }
}

trait CollectionsEncoder {

  def instance[A](f: A => JsLikeData): Encoder[A] = new Encoder[A] {
    override def apply(a: A): JsLikeData = f(a)
  }

  implicit def arrEncoder[A](implicit u: Encoder[A]): Encoder[Array[A]] = instance(arr => {
    val values = arr.map(i => u(i))
    JsLikeList(values)
  })

  implicit def seqEncoder[A](implicit u: Encoder[A]): Encoder[Seq[A]] = instance(arr => {
    val values = arr.map(i => u(i))
    JsLikeList(values)
  })

  implicit def mapEncoder[A](implicit u: Encoder[A]): Encoder[Map[String, A]] = instance(arr => {
    val values = arr.mapValues(i => u(i))
    JsLikeMap(values)
  })

}

trait DefaultEncoders extends PrimitiveEncoders with CollectionsEncoder

object DefaultEncoders extends DefaultEncoders {

  def apply[A](implicit enc: Encoder[A]): Encoder[A] = enc

}
