package mist.api.encoding

import mist.api.data.{JsMap, JsNull}
import shadedshapeless.labelled.FieldType
import shadedshapeless._

/**
  * Obtain defaults values from case classes and patch incoming js by encoding them into jsMap
  */
trait DefaultsPatcher[A] {
  def apply(js: JsMap): JsMap
}

object DefaultsPatcher {

  trait InternalPatcher[A] {
    def apply(a: A, js: JsMap): JsMap
  }
  object InternalPatcher {
    def create[A](f: (A, JsMap) => JsMap): InternalPatcher[A] = new InternalPatcher[A] {
      override def apply(a: A, js: JsMap): JsMap = f(a, js)
    }

    implicit val hHNilPatcher: InternalPatcher[HNil] = InternalPatcher.create((_, js) => js)

    implicit def hlistPatcher[K <: Symbol, H, T <: HList](implicit
      witness: Witness.Aux[K],
      LEnc: Lazy[JsEncoder[H]],
      tPath: InternalPatcher[T]
    ): InternalPatcher[FieldType[K, H] :: T] = {
      val enc = LEnc.value
      val key = witness.value.name
      InternalPatcher.create[FieldType[K, H] :: T]((hList, js) => {
        val patched = js.fieldValue(key) match {
          case JsNull => JsMap((key -> enc(hList.head)) +: js.fields: _*)
          case _ => js
        }
        tPath(hList.tail, patched)
      })
    }
  }


  implicit def labelled[A, H <: HList](implicit
    defaults: Default.AsRecord.Aux[A, H],
    intPatch: InternalPatcher[H]
  ): DefaultsPatcher[A] = new DefaultsPatcher[A] {
    override def apply(js: JsMap): JsMap = {
      intPatch.apply(defaults(), js)
    }
  }

  def apply[A](implicit patcher: DefaultsPatcher[A]): DefaultsPatcher[A] = patcher
}
