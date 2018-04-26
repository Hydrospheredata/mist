package mist.api.encoding

import mist.api.data.{JsData, JsMap}
import shadedshapeless.labelled.FieldType
import shadedshapeless._

import scala.annotation.implicitNotFound

@implicitNotFound(msg =
  "Couldn't find mist.api.encoding.ObjectEncoder[${A}]" +
  " Ensure that JsEncoder instances exists for it's fields" +
  " or add `import mist.api.encoding.defaults._`"
)
trait ObjectEncoder[A] { self =>
  def apply(a : A): JsMap
}

object ObjectEncoder {

  def apply[A](implicit enc: ObjectEncoder[A]): ObjectEncoder[A] = enc

  def create[A](f: A => JsMap): ObjectEncoder[A] = new ObjectEncoder[A] {
    override def apply(a: A): JsMap = f(a)
  }

  implicit val hNilEnc: ObjectEncoder[HNil] = ObjectEncoder.create[HNil](_ => JsMap.empty)

  implicit def hlistExt[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    lHenc: Lazy[JsEncoder[H]],
    tEnc: ObjectEncoder[T]
  ): ObjectEncoder[FieldType[K, H] :: T] = {
    val hEnc = lHenc.value
    val key = witness.value.name
    ObjectEncoder.create[FieldType[K, H] :: T](hlist => {
      val h = hEnc(hlist.head)
      val t = tEnc(hlist.tail)
      val values = (key -> h) +: t.fields
      JsMap(values: _*)
    })
  }

  implicit def labelled[A, H <: HList](implicit labGen: LabelledGeneric.Aux[A, H], enc: ObjectEncoder[H]): ObjectEncoder[A] =
    ObjectEncoder.create(a => enc(labGen.to(a)))
}
