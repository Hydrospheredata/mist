package mist.api.encoding

import mist.api._
import mist.api.data.{JsData, JsMap, JsNull}
import shadedshapeless.labelled.FieldType
import shadedshapeless._
import shadedshapeless.record._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

@implicitNotFound("Couldn't find ObjectExtractor instances for ${A}. Ensure that there are JsExtractor instances for every field type of ${A}")
trait ObjectExtractor[A] {
  def `type`: MObj
  def apply(js: JsData): Extraction[A]
}

object ObjectExtractor {

  def apply[A](argType: MObj)(f: JsMap => Extraction[A]): ObjectExtractor[A] = new ObjectExtractor[A] {
    def apply(js: JsData): Extraction[A] = js match {
      case m: JsMap => f(m)
      case other =>
        Failed.InvalidType(argType.toString, other.toString)
    }
    val `type`: MObj = argType
  }

  implicit val hNilExt: ObjectExtractor[HNil] = ObjectExtractor(MObj.empty)(_ => Extracted(HNil))

  implicit def hlistExt[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    lHExt: Lazy[JsExtractor[H]],
    tExt: ObjectExtractor[T]
  ): ObjectExtractor[FieldType[K, H] :: T] = {
    val key = witness.value.name
    val hExt = lHExt.value.transformFailure(f => Failed.InvalidField(key, f))
    val headType = witness.value.name -> hExt.`type`
    val `type` = MObj(headType +: tExt.`type`.fields)

    ObjectExtractor(`type`)(map => {
      val headV = map.fieldValue(key)
      (hExt(headV), tExt(map)) match {
        case (Extracted(h), Extracted(t)) => Extracted((h :: t).asInstanceOf[FieldType[K, H] :: T])
        case (Extracted(h), f: Failed) => f
        case (f: Failed, Extracted(t)) => f
        case (f1: Failed, f2: Failed) => Failed.toComplex(f1, f2)
      }
    })
  }

  implicit def labelled[A, H <: HList](
    implicit
    labGen: LabelledGeneric.Aux[A, H],
    clzTag: ClassTag[A],
    ext: ObjectExtractor[H]
  ): ObjectExtractor[A] =
    ObjectExtractor(ext.`type`)(map => {
      ext(map) match {
        case Extracted(h) => Extracted(labGen.from(h))
        case f: Failed => Failed.IncompleteObject(clzTag.runtimeClass.getName, f)
      }
    })
}
