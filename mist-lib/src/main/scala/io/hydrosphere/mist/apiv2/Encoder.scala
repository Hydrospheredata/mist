package io.hydrosphere.mist.apiv2

import org.json4s._

trait Encoder[A] extends Serializable {

  //TODO: json4s?
  def apply(a : A): JValue

}

trait DefaultEncoders {

  def create[A](f: A => JValue): Encoder[A] = new Encoder[A] {
    override def apply(a: A): JValue = f(a)
  }

  val intEncoder = create[Int](i => JInt(i))
  val stringEncoder = create[String](s => JString(s))

  def arrEncoder[A](implicit underlying: Encoder[A]): Encoder[Array[A]] = create(arr => {
    val values = arr.map(v => underlying(v))
    JArray(values.toList)
  })

  def seqEncoder[A](implicit underlying: Encoder[A]): Encoder[Seq[A]] = create(seq => {
    val values = seq.map(v => underlying(v))
    JArray(values.toList)
  })

}
