package mist.api.encoding

import mist.api.data.{JsData, JsLikeMap}
import shadedshapeless.labelled.FieldType
import shadedshapeless._

trait ObjectEncoder[A] { self =>

  def apply(a : A): JsData

  final def contramap[B](f: B => A): JsEncoder[B] = new JsEncoder[B] {
    def apply(b: B): JsData = self(f(b))
  }
}

object ObjectEncoder {

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

  implicit def labelled[A, H <: HList](implicit labGen: LabelledGeneric.Aux[A, H], enc: ObjEncoder[H]): ObjEncoder[A] =
    ObjEncoder(a => enc(labGen.to(a)))
}
