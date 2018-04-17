package mist.api.codecs

import mist.api.args._
import mist.api.data._
import shadedshapeless._
import shadedshapeless.labelled.FieldType

import scala.reflect.ClassTag

sealed trait Extraction[+A] { self =>

  def map[B](f: A => B): Extraction[B] = self match {
    case ExtractedX(a) => ExtractedX(f(a))
    case f:FailedExt => f.asInstanceOf[Extraction[B]]
  }

  def flatMap[B](f: A => Extraction[B]): Extraction[B] = self match {
    case ExtractedX(a) => f(a)
    case f:FailedExt => f.asInstanceOf[Extraction[B]]
  }

  def isFailed: Boolean = self match {
    case ExtractedX(a) => false
    case f:FailedExt => true
  }

  def isExtracted: Boolean = !isFailed

}

final case class ExtractedX[+A](value: A) extends Extraction[A]

trait FailedExt extends Extraction[Nothing]
object FailedExt {

  final case class InvalidType(expected: String, got: String) extends FailedExt
  final case class ComplexFailure(failures: Seq[FailedExt]) extends FailedExt
  final case class IncompleteObject(clazz: String, failure: FailedExt) extends FailedExt

  def invalidType(expected: String, got: String): InvalidType = InvalidType(expected, got)

  def toComplex(f1: FailedExt, f2: FailedExt): ComplexFailure = (f1, f2) match {
    case (ComplexFailure(in1), ComplexFailure(in2)) => ComplexFailure(in1 ++ in2)
    case (ComplexFailure(in), f:FailedExt) => ComplexFailure(in :+ f)
    case (f:FailedExt, ComplexFailure(in)) => ComplexFailure(f +: in)
    case (f1: FailedExt, f2: FailedExt) => ComplexFailure(Seq(f1, f2))
  }
}

object Extraction {

  def tryExtract[A](f: => A)(handleF: Throwable => FailedExt): Extraction[A] = {
    try {
      ExtractedX(f)
    } catch {
      case e: Throwable => handleF(e)
    }
  }

}

trait Extractor[A] { self =>
  def `type`: ArgType
  def apply(js: JsLikeData): Extraction[A]

  final def map[B](f: A => B): Extractor[B] = new Extractor[B] {
    def apply(js: JsLikeData): Extraction[B] = self(js).map(f)
    def `type`: ArgType = self.`type`
  }

  final def flatMap[B](f: A => Extractor[B]) = new Extractor[B] {
    def apply(js: JsLikeData): Extraction[B] = self(js) match {
      case ExtractedX(a) => f(a)(js)
      case f: FailedExt => f.asInstanceOf[Extraction[B]]
    }
    def `type`: ArgType = self.`type`
  }

  final def andThen[B](f: A => Extraction[B]) = new Extractor[B] {
    def apply(js: JsLikeData): Extraction[B] = self(js).flatMap(f)
    def `type`: ArgType = self.`type`
  }

  final def recover(f: FailedExt => A): Extractor[A] = new Extractor[A] {
    def apply(js: JsLikeData): Extraction[A] = self(js) match {
      case failed:FailedExt => ExtractedX(f(failed))
      case ext => ext
    }
    def `type`: ArgType = self.`type`
  }

  final def orElse[AA <: A](f: => AA): Extractor[AA] = new Extractor[AA] {
    def apply(js: JsLikeData): Extraction[AA] = self(js) match {
      case ext if ext.isFailed => ExtractedX(f)
      case ext => ext.asInstanceOf[Extraction[AA]]
    }
    def `type`: ArgType = self.`type`
  }

}

trait RootExt[A] extends Extractor[A] {
  def `type`: RootArgType
  def apply(js: JsLikeData): Extraction[A]
}

trait ObjExt[A] extends RootExt[A] {
  def `type`: MObj
  def apply(js: JsLikeData): Extraction[A]
}

object Extractor {
  def plain[A](argType: ArgType)(f: JsLikeData => Extraction[A]): Extractor[A] = new Extractor[A] {
    def apply(js: JsLikeData): Extraction[A] = f(js)
    val `type`: ArgType = argType
  }

  def root[A](argType: RootArgType)(f: JsLikeMap => Extraction[A]): RootExt[A] = new RootExt[A] {
    def apply(js: JsLikeData): Extraction[A] = js match {
      case m: JsLikeMap => f(m)
      case other =>
        FailedExt.InvalidType(argType.toString, other.toString)
    }
    val `type`: RootArgType = argType
  }

  def obj[A](argType: MObj)(f: JsLikeMap => Extraction[A]): ObjExt[A] = new ObjExt[A] {
    def apply(js: JsLikeData): Extraction[A] = js match {
      case m: JsLikeMap => f(m)
      case other =>
        FailedExt.InvalidType(argType.toString, other.toString)
    }
    val `type`: MObj = argType
  }
}

trait PrimitiveExtractorInstances {

  implicit val boolExt: Extractor[Boolean] = Extractor.plain(MBoolean){
    case b: JsLikeBoolean => ExtractedX(b.b)
    case oth => FailedExt.invalidType("Boolean", oth.toString)
  }

  implicit val intExt: Extractor[Int] = Extractor.plain(MInt) {
    case n: JsLikeNumber => Extraction.tryExtract(n.v.toIntExact)(_ => FailedExt.invalidType("Int", n.v.toString()))
    case oth => FailedExt.invalidType("Int", oth.toString)
  }

  implicit val longExt: Extractor[Long] = Extractor.plain(MInt) {
    case n: JsLikeNumber => Extraction.tryExtract(n.v.toLongExact)(_ => FailedExt.invalidType("Long", n.v.toString()))
    case oth => FailedExt.invalidType("Long", oth.toString)
  }

  implicit val floatExt: Extractor[Float] = Extractor.plain(MDouble) {
    case n: JsLikeNumber => ExtractedX(n.v.toFloat)
    case oth => FailedExt.invalidType("Float", oth.toString)
  }

  implicit val doubleExt: Extractor[Double] = Extractor.plain(MDouble) {
    case n: JsLikeNumber => ExtractedX(n.v.toDouble)
    case oth => FailedExt.invalidType("Double", oth.toString)
  }

  implicit val stringExt: Extractor[String] = Extractor.plain(MString) {
    case s: JsLikeString => ExtractedX(s.s)
    case oth => FailedExt.invalidType("String", oth.toString)
  }

}

trait CollectionExtractorInstances {

  implicit def optExt[A](implicit ext: Extractor[A]): Extractor[Option[A]] = Extractor.plain(MOption(ext.`type`)) {
    case JsLikeNull => ExtractedX(None)
    case value => ext(value).map(Some(_))
  }

  implicit def seqExt[A](implicit ext: Extractor[A]): Extractor[Seq[A]] = Extractor.plain(MList(ext.`type`)) {
    case jsList: JsLikeList =>
      val elems = jsList.list.map(ext.apply)
      if (elems.exists(_.isFailed))
        FailedExt.invalidType(s"List[${ext.`type`}]", jsList.toString)
      else
        ExtractedX(elems.collect({case ExtractedX(v) => v}))
    case value => FailedExt.invalidType(s"List[${ext.`type`}]", value.toString)
  }

  implicit def listExt[A](implicit ext: Extractor[Seq[A]]): Extractor[List[A]] = ext.map(_.toList)
  implicit def arrExt[A](implicit ext: Extractor[Seq[A]], ct: ClassTag[A]): Extractor[Array[A]] = ext.map(_.toArray)

  implicit def mapExt[A](implicit ext: Extractor[A]): Extractor[Map[String, A]] = Extractor.plain(MMap(MString, ext.`type`)) {
    case jsMap: JsLikeMap =>
      val elems = jsMap.fields.map({case (k, v) => ext(v).map(e => (k, e))})
      if (elems.exists(_.isFailed))
        FailedExt.invalidType(s"Map[String, ${ext.`type`}]", jsMap.toString)
      else
        ExtractedX(elems.collect({case ExtractedX(v) => v}).toMap)

    case value => FailedExt.invalidType(s"Map[String, ${ext.`type`}]", value.toString)
  }
}


trait GenericExtractorInstances {

  implicit val hNilExt: ObjExt[HNil] = Extractor.obj(MObj.empty)(_ => ExtractedX(HNil))

  implicit def hlistExt[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    LHExt: Lazy[Extractor[H]],
    tExt: ObjExt[T]
  ): ObjExt[FieldType[K, H] :: T] = {
    val hExt = LHExt.value
    val key = witness.value.name
    val headType = witness.value.name -> hExt.`type`
    val `type` = MObj(headType +: tExt.`type`.fields)

    Extractor.obj(`type`)(map => {
      val headV = map.fieldValue(key)
      (hExt(headV), tExt(map)) match {
        case (ExtractedX(h), ExtractedX(t)) => ExtractedX((h :: t).asInstanceOf[FieldType[K, H] :: T])
        case (ExtractedX(h), f: FailedExt) => f
        case (f: FailedExt, ExtractedX(t)) => f
        case (f1: FailedExt, f2: FailedExt) => FailedExt.toComplex(f1, f2)
      }
    })
  }

  implicit def labelled[A, H <: HList](
    implicit
    labGen: LabelledGeneric.Aux[A, H],
    clzTag: ClassTag[A],
    ext: ObjExt[H]
  ): ObjExt[A] =
    Extractor.obj(ext.`type`)(map => ext(map) match {
      case ExtractedX(h) => ExtractedX(labGen.from(h))
      case f: FailedExt => FailedExt.IncompleteObject(clzTag.runtimeClass.getName, f)
    })

}

