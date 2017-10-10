package mist.api
import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find Encoder instance for ${A}")
trait Encoder[A] extends Serializable {

  def apply(a : A): MData

}

trait PrimitiveEncoders {

  implicit class EncoderSyntax[A](val a: A)(implicit enc: Encoder[A]) {
    def encode(): Any = enc(a)
  }

  implicit object IntEncoder extends Encoder[Int] {
    def apply(a: Int): MData = MInt(a)
  }

  implicit object StringEncoder extends Encoder[String] {
    def apply(a: String): MData = MString(a)
  }

  implicit object BooleanEncoder extends Encoder[Boolean] {
    def apply(b: Boolean): MData = MBoolean(b)
  }

  implicit object DoubleEncoder extends Encoder[Double] {
    def apply(d: Double): MData = MDouble(d)
  }
}

trait CollectionsEncoder {

  def instance[A](f: A => MData): Encoder[A] = new Encoder[A] {
    override def apply(a: A): MData = f(a)
  }

  implicit def arrEncoder[A](implicit u: Encoder[A]): Encoder[Array[A]] = instance(arr => {
    val values = arr.map(i => u(i))
    MList(values)
  })

  implicit def seqEncoder[A](implicit u: Encoder[A]): Encoder[Seq[A]] = instance(arr => {
    val values = arr.map(i => u(i))
    MList(values)
  })

  implicit def mapEncoder[A](implicit u: Encoder[A]): Encoder[Map[String, A]] = instance(arr => {
    val values = arr.mapValues(i => u(i))
    MMap(values)
  })

}

trait DefaultEncoders extends PrimitiveEncoders with CollectionsEncoder

object DefaultEncoders extends DefaultEncoders {

  def apply[A](implicit enc: Encoder[A]): Encoder[A] = enc

}
