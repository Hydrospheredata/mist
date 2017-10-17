package mist.api


import mist.api.data._

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find Encoder instance for ${A}")
trait Encoder[A] extends Serializable {

  def apply(a : A): JsLikeData

}

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

trait DefaultEncoders extends PrimitiveEncoders
  with CollectionsEncoder
  with SparkDataEncoders

object DefaultEncoders extends DefaultEncoders {

  def apply[A](implicit enc: Encoder[A]): Encoder[A] = enc

}
