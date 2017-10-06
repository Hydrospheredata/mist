package mist.api

import org.json4s._

import scala.annotation.implicitNotFound

trait Encoder[A] extends Serializable {

  //TODO: json4s?
  def apply(a : A): Any

}

trait DefaultEncoders {

  def createE[A](f: A => Any): Encoder[A] = new Encoder[A] {
    override def apply(a: A): Any = f(a)
  }

  //implicit val intEncoder: Encoder[Int] = createE[Int](i => JInt(i))
  implicit val intEncoder: Encoder[Int] = createE[Int](i => i)
  //implicit val stringEncoder: Encoder[String] = createE[String](s => JString(s))
  implicit val stringEncoder: Encoder[String] = createE[String](s => s)

//  implicit def arrEncoder[A](implicit underlying: Encoder[A]): Encoder[Array[A]] = createE(arr => {
//    val values = arr.map(v => underlying(v))
//    JArray(values.toList)
//  })
    implicit def arrEncoder[A](implicit underlying: Encoder[A]): Encoder[Array[A]] = createE(arr => {
      val values = arr.map(v => underlying(v))
      values.toList
    })

//  implicit def seqEncoder[A](implicit underlying: Encoder[A]): Encoder[Seq[A]] = createE(seq => {
//    val values = seq.map(v => underlying(v))
//    JArray(values.toList)
//  })
  implicit def seqEncoder[A](implicit underlying: Encoder[A]): Encoder[Seq[A]] = createE(seq => {
    val values = seq.map(v => underlying(v))
    values.toList
  })

}

object DefaultEncoders extends DefaultEncoders
