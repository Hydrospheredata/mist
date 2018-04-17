package mist.api.codecs

import mist.api.data._
import shadedshapeless._
import shadedshapeless.labelled.FieldType

import scala.annotation.implicitNotFound

@implicitNotFound(msg = "Could not find Encoder instance for ${A}")
trait Encoder[A] { self =>

  def apply(a : A): JsLikeData

  final def contramap[B](f: B => A): Encoder[B] = new Encoder[B] {
    def apply(b: B): JsLikeData = self(f(b))
  }
}

object Encoder {
  def apply[A](f: A => JsLikeData): Encoder[A] = new Encoder[A] {
    override def apply(a: A): JsLikeData = f(a)
  }
}

trait PrimitiveEncoderInstances {

  implicit val unitEnc: Encoder[Unit] = Encoder(_ => JsLikeUnit)

  implicit val booleanEnc: Encoder[Boolean] = Encoder(JsLikeBoolean)

  implicit val shortEnc: Encoder[Short] = Encoder(n => JsLikeNumber(n.toInt))
  implicit val intEnc: Encoder[Int] = Encoder(i => JsLikeNumber(i))
  implicit val longEnc: Encoder[Long] = Encoder(i => JsLikeNumber(i))
  implicit val floatEnc: Encoder[Float] = Encoder(f => JsLikeNumber(f.toDouble))
  implicit val doubleEnc: Encoder[Double] = Encoder(d => JsLikeNumber(d))

  implicit val stringEnc: Encoder[String] = Encoder(JsLikeString)

}

trait CollectionEncoderInstances {

  implicit def seqEnc[A](implicit enc: Encoder[A]): Encoder[Seq[A]] = Encoder(seq => JsLikeList(seq.map(v => enc(v))))
  implicit def arrEnc[A](implicit enc: Encoder[Seq[A]]): Encoder[Array[A]] = enc.contramap(_.toSeq)
  implicit def optEnc[A](implicit enc: Encoder[A]): Encoder[Option[A]] = Encoder {
    case Some(a) => enc(a)
    case None => JsLikeNull
  }
  implicit def mapEnc[A](implicit enc: Encoder[A]): Encoder[Map[String, A]] = Encoder(m => JsLikeMap(m.mapValues(enc.apply)))
}

trait DefaultEncoderInstances extends PrimitiveEncoderInstances with CollectionsEncoder
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
    LHenc: Lazy[Encoder[H]],
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
  ): Encoder[A] = Encoder(a => enc(labGen.to(a)))

}
object GenericEncoderInstances extends GenericEncoderInstances
