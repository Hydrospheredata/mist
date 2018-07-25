package mist.api.encoding

import mist.api.data._

import scala.annotation.implicitNotFound

@implicitNotFound("Couldn't find JsEncoder instance for ${A}. Check that function return value type has encoder instance or import default `mist.api.encoder.defaults._`")
trait JsEncoder[A] { self =>
  def apply(a : A): JsData
}

object JsEncoder {
  def apply[A](f: A => JsData): JsEncoder[A] = new JsEncoder[A] {
    override def apply(a: A): JsData = f(a)
  }
}

trait defaultEncoders {

  implicit val identity: JsEncoder[JsData] = JsEncoder(data => data)
  implicit val unitEnc: JsEncoder[Unit] = JsEncoder(_ => JsUnit)

  implicit val booleanEnc: JsEncoder[Boolean] = JsEncoder(b => JsBoolean(b))
  implicit val shortEnc: JsEncoder[Short] = JsEncoder(n => JsNumber(n.toInt))
  implicit val intEnc: JsEncoder[Int] = JsEncoder(i => JsNumber(i))
  implicit val longEnc: JsEncoder[Long] = JsEncoder(i => JsNumber(i))
  implicit val floatEnc: JsEncoder[Float] = JsEncoder(f => JsNumber(f.toDouble))
  implicit val doubleEnc: JsEncoder[Double] = JsEncoder(d => JsNumber(d))
  implicit val stringEnc: JsEncoder[String] = JsEncoder(s => JsString(s))

  implicit def seqEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Seq[A]] = JsEncoder(seq => JsList(seq.map(v => enc(v))))
  implicit def listEnc[A](implicit enc: JsEncoder[A]): JsEncoder[List[A]] = JsEncoder(list => JsList(list.map(v => enc(v))))
  implicit def arrEnc[A](implicit enc: JsEncoder[Seq[A]]): JsEncoder[Array[A]] = JsEncoder(arr => enc(arr.toSeq))
  implicit def optEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Option[A]] = JsEncoder {
    case Some(a) => enc(a)
    case None => JsNull
  }
  implicit def mapEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Map[String, A]] = JsEncoder(m => JsMap(m.mapValues(enc.apply)))
}

object defaultEncoders extends defaultEncoders
