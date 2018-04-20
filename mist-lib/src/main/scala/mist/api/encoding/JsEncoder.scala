package mist.api.encoding

import mist.api.data._
import shadedshapeless._
import shadedshapeless.labelled.FieldType

//TODO full message
trait JsEncoder[A] { self =>

  def apply(a : A): JsData

  final def contramap[B](f: B => A): JsEncoder[B] = new JsEncoder[B] {
    def apply(b: B): JsData = self(f(b))
  }
}

object JsEncoder {
  def apply[A](f: A => JsData): JsEncoder[A] = new JsEncoder[A] {
    override def apply(a: A): JsData = f(a)
  }
}

trait DefaultEncoders {

  implicit val unitEnc: JsEncoder[Unit] = JsEncoder(_ => JsUnit)

  implicit val booleanEnc: JsEncoder[Boolean] = JsEncoder(b => JsBoolean(b))
  implicit val shortEnc: JsEncoder[Short] = JsEncoder(n => JsNumber(n.toInt))
  implicit val intEnc: JsEncoder[Int] = JsEncoder(i => JsNumber(i))
  implicit val longEnc: JsEncoder[Long] = JsEncoder(i => JsNumber(i))
  implicit val floatEnc: JsEncoder[Float] = JsEncoder(f => JsNumber(f.toDouble))
  implicit val doubleEnc: JsEncoder[Double] = JsEncoder(d => JsNumber(d))
  implicit val stringEnc: JsEncoder[String] = JsEncoder(JsString)

  implicit def seqEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Seq[A]] = JsEncoder(seq => JsList(seq.map(v => enc(v))))
  implicit def arrEnc[A](implicit enc: JsEncoder[Seq[A]]): JsEncoder[Array[A]] = enc.contramap(_.toSeq)
  implicit def optEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Option[A]] = JsEncoder {
    case Some(a) => enc(a)
    case None => JsNull
  }
  implicit def mapEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Map[String, A]] = JsEncoder(m => JsMap(m.mapValues(enc.apply)))
}

object DefaultEncoders extends DefaultEncoders

//trait ObjEncoder[A] {
//  def apply(a: A): JsMap
//}
//
//object ObjEncoder {
//
//  def apply[A](f: A => JsMap): ObjEncoder[A] = new ObjEncoder[A] {
//    override def apply(a: A): JsMap = f(a)
//  }
//
//  implicit val hNilEnc: ObjEncoder[HNil] = ObjEncoder[HNil](_ => JsMap.empty)
//
//  implicit def hlistExt[K <: Symbol, H, T <: HList](implicit
//    witness: Witness.Aux[K],
//    LHenc: Lazy[JsEncoder[H]],
//    tEnc: ObjEncoder[T]
//  ): ObjEncoder[FieldType[K, H] :: T] = {
//    val hEnc = LHenc.value
//    val key = witness.value.name
//    ObjEncoder[FieldType[K, H] :: T](hlist => {
//      val h = hEnc(hlist.head)
//      val t = tEnc(hlist.tail)
//      val values = (key -> h) +: t.fields
//      JsMap(values: _*)
//    })
//  }
//
//}
//
//trait GenericEncoderInstances {
//
//  implicit def labelled[A, H <: HList](implicit
//    labGen: LabelledGeneric.Aux[A, H],
//    enc: ObjEncoder[H]
//  ): JsEncoder[A] = JsEncoder(a => enc(labGen.to(a)))
//
//}
//object GenericEncoderInstances extends GenericEncoderInstances
