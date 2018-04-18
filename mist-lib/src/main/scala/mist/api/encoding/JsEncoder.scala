package mist.api.encoding

import mist.api.data._
import shadedshapeless._
import shadedshapeless.labelled.FieldType

import scala.annotation.implicitNotFound

//TODO full message
@implicitNotFound(msg = "Could not find JsEncoder instance for ${A}")
trait JsEncoder[A] { self =>

  def apply(a : A): JsLikeData

  final def contramap[B](f: B => A): JsEncoder[B] = new JsEncoder[B] {
    def apply(b: B): JsLikeData = self(f(b))
  }
}

object JsEncoder {
  def apply[A](f: A => JsLikeData): JsEncoder[A] = new JsEncoder[A] {
    override def apply(a: A): JsLikeData = f(a)
  }
}

trait PrimitiveEncoderInstances {

  implicit val unitEnc: JsEncoder[Unit] = JsEncoder(_ => JsLikeUnit)

  implicit val booleanEnc: JsEncoder[Boolean] = JsEncoder(JsLikeBoolean)

  implicit val shortEnc: JsEncoder[Short] = JsEncoder(n => JsLikeNumber(n.toInt))
  implicit val intEnc: JsEncoder[Int] = JsEncoder(i => JsLikeNumber(i))
  implicit val longEnc: JsEncoder[Long] = JsEncoder(i => JsLikeNumber(i))
  implicit val floatEnc: JsEncoder[Float] = JsEncoder(f => JsLikeNumber(f.toDouble))
  implicit val doubleEnc: JsEncoder[Double] = JsEncoder(d => JsLikeNumber(d))

  implicit val stringEnc: JsEncoder[String] = JsEncoder(JsLikeString)

}

trait CollectionEncoderInstances extends PrimitiveEncoderInstances {

  implicit def seqEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Seq[A]] = JsEncoder(seq => JsLikeList(seq.map(v => enc(v))))
  implicit def arrEnc[A](implicit enc: JsEncoder[Seq[A]]): JsEncoder[Array[A]] = enc.contramap(_.toSeq)
  implicit def optEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Option[A]] = JsEncoder {
    case Some(a) => enc(a)
    case None => JsLikeNull
  }
  implicit def mapEnc[A](implicit enc: JsEncoder[A]): JsEncoder[Map[String, A]] = JsEncoder(m => JsLikeMap(m.mapValues(enc.apply)))
}

trait DefaultEncoderInstances extends CollectionEncoderInstances
object DefaultEncoderInstances extends DefaultEncoderInstances

trait ObjEncoder[A] {
  def apply(a: A): JsLikeMap
}

object ObjEncoder {

  def apply[A](f: A => JsLikeMap): ObjEncoder[A] = new ObjEncoder[A] {
    override def apply(a: A): JsLikeMap = f(a)
  }

  implicit val hNilEnc: ObjEncoder[HNil] = ObjEncoder[HNil](_ => JsLikeMap.empty)

  implicit def hlistExt[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    LHenc: Lazy[JsEncoder[H]],
    tEnc: ObjEncoder[T]
  ): ObjEncoder[FieldType[K, H] :: T] = {
    val hEnc = LHenc.value
    val key = witness.value.name
    ObjEncoder[FieldType[K, H] :: T](hlist => {
      val h = hEnc(hlist.head)
      val t = tEnc(hlist.tail)
      val values = (key -> h) +: t.fields
      JsLikeMap(values: _*)
    })
  }

}

trait GenericEncoderInstances {

  implicit def labelled[A, H <: HList](implicit
    labGen: LabelledGeneric.Aux[A, H],
    enc: ObjEncoder[H]
  ): JsEncoder[A] = JsEncoder(a => enc(labGen.to(a)))

}
object GenericEncoderInstances extends GenericEncoderInstances
