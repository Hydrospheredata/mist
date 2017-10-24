package mist.api.encoding
import mist.api.data._

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
