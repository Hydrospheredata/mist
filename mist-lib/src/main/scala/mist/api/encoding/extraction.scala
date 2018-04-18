package mist.api.encoding

import mist.api.args._
import mist.api.data._
import shadedshapeless._
import shadedshapeless.labelled.FieldType

import scala.reflect.ClassTag

sealed trait Extraction[+A] { self =>

  def map[B](f: A => B): Extraction[B] = self match {
    case Extracted(a) => Extracted(f(a))
    case f:FailedExt => f.asInstanceOf[Extraction[B]]
  }

  def flatMap[B](f: A => Extraction[B]): Extraction[B] = self match {
    case Extracted(a) => f(a)
    case f:FailedExt => f.asInstanceOf[Extraction[B]]
  }

  def isFailed: Boolean = self match {
    case Extracted(a) => false
    case f:FailedExt => true
  }

  def isExtracted: Boolean = !isFailed

}

final case class Extracted[+A](value: A) extends Extraction[A]

trait FailedExt extends Extraction[Nothing]
object FailedExt {

  final case class InternalError(msg: String) extends FailedExt
  final case class InvalidValue(msg: String) extends FailedExt
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
      Extracted(f)
    } catch {
      case e: Throwable => handleF(e)
    }
  }

}

trait JsExtractor[A] { self =>
  def `type`: ArgType
  def apply(js: JsLikeData): Extraction[A]

  final def map[B](f: A => B): JsExtractor[B] = new JsExtractor[B] {
    def apply(js: JsLikeData): Extraction[B] = self(js).map(f)
    def `type`: ArgType = self.`type`
  }

  final def flatMap[B](f: A => JsExtractor[B]) = new JsExtractor[B] {
    def apply(js: JsLikeData): Extraction[B] = self(js) match {
      case Extracted(a) => f(a)(js)
      case f: FailedExt => f.asInstanceOf[Extraction[B]]
    }
    def `type`: ArgType = self.`type`
  }

  final def andThen[B](f: A => Extraction[B]) = new JsExtractor[B] {
    def apply(js: JsLikeData): Extraction[B] = self(js).flatMap(f)
    def `type`: ArgType = self.`type`
  }

  final def recover(f: FailedExt => A): JsExtractor[A] = new JsExtractor[A] {
    def apply(js: JsLikeData): Extraction[A] = self(js) match {
      case failed:FailedExt => Extracted(f(failed))
      case ext => ext
    }
    def `type`: ArgType = self.`type`
  }

  final def orElse[AA <: A](f: => AA): JsExtractor[AA] = new JsExtractor[AA] {
    def apply(js: JsLikeData): Extraction[AA] = self(js) match {
      case ext if ext.isFailed => Extracted(f)
      case ext => ext.asInstanceOf[Extraction[AA]]
    }
    def `type`: ArgType = self.`type`
  }

}

trait RootExt[A] extends JsExtractor[A] {
  def `type`: RootArgType
  def apply(js: JsLikeData): Extraction[A]
}

trait ObjExt[A] extends RootExt[A] {
  def `type`: MObj
  def apply(js: JsLikeData): Extraction[A]
}

object JsExtractor {
  def plain[A](argType: ArgType)(f: JsLikeData => Extraction[A]): JsExtractor[A] = new JsExtractor[A] {
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

  import java.{lang => jl, util => ju}

  implicit val boolExt: JsExtractor[Boolean] = JsExtractor.plain(MBoolean){
    case b: JsLikeBoolean => Extracted(b.b)
    case oth => FailedExt.invalidType("Boolean", oth.toString)
  }
  implicit val jBoolExt: JsExtractor[jl.Boolean] = boolExt.map(b => b)

  implicit val intExt: JsExtractor[Int] = JsExtractor.plain(MInt) {
    case n: JsLikeNumber => Extraction.tryExtract(n.v.toIntExact)(_ => FailedExt.invalidType("Int", n.v.toString()))
    case oth => FailedExt.invalidType("Int", oth.toString)
  }

  implicit val jlIntExt: JsExtractor[jl.Integer] = intExt.map(i => i)

  implicit val longExt: JsExtractor[Long] = JsExtractor.plain(MInt) {
    case n: JsLikeNumber => Extraction.tryExtract(n.v.toLongExact)(_ => FailedExt.invalidType("Long", n.v.toString()))
    case oth => FailedExt.invalidType("Long", oth.toString)
  }

  implicit val floatExt: JsExtractor[Float] = JsExtractor.plain(MDouble) {
    case n: JsLikeNumber => Extracted(n.v.toFloat)
    case oth => FailedExt.invalidType("Float", oth.toString)
  }

  implicit val doubleExt: JsExtractor[Double] = JsExtractor.plain(MDouble) {
    case n: JsLikeNumber => Extracted(n.v.toDouble)
    case oth => FailedExt.invalidType("Double", oth.toString)
  }

  implicit val jDoubleExt: JsExtractor[jl.Double] = doubleExt.map(d => d)

  implicit val stringExt: JsExtractor[String] = JsExtractor.plain(MString) {
    case s: JsLikeString => Extracted(s.s)
    case oth => FailedExt.invalidType("String", oth.toString)
  }

}

trait CollectionExtractorInstances extends PrimitiveExtractorInstances {

  implicit def optExt[A](implicit ext: JsExtractor[A]): JsExtractor[Option[A]] = JsExtractor.plain(MOption(ext.`type`)) {
    case JsLikeNull => Extracted(None)
    case value => ext(value).map(Some(_))
  }

  implicit def seqExt[A](implicit ext: JsExtractor[A]): JsExtractor[Seq[A]] = JsExtractor.plain(MList(ext.`type`)) {
    case jsList: JsLikeList =>
      val elems = jsList.list.map(ext.apply)
      if (elems.exists(_.isFailed))
        FailedExt.invalidType(s"List[${ext.`type`}]", jsList.toString)
      else
        Extracted(elems.collect({case Extracted(v) => v}))
    case value => FailedExt.invalidType(s"List[${ext.`type`}]", value.toString)
  }

  implicit def listExt[A](implicit ext: JsExtractor[Seq[A]]): JsExtractor[List[A]] = ext.map(_.toList)
  implicit def arrExt[A](implicit ext: JsExtractor[Seq[A]], ct: ClassTag[A]): JsExtractor[Array[A]] = ext.map(_.toArray)

  implicit def mapExt[A](implicit ext: JsExtractor[A]): JsExtractor[Map[String, A]] = JsExtractor.plain(MMap(MString, ext.`type`)) {
    case jsMap: JsLikeMap =>
      val elems = jsMap.fields.map({case (k, v) => ext(v).map(e => (k, e))})
      if (elems.exists(_.isFailed))
        FailedExt.invalidType(s"Map[String, ${ext.`type`}]", jsMap.toString)
      else
        Extracted(elems.collect({case Extracted(v) => v}).toMap)

    case value => FailedExt.invalidType(s"Map[String, ${ext.`type`}]", value.toString)
  }
}

trait DefaultExtractorInstances extends CollectionExtractorInstances
object DefaultExtractorInstances extends DefaultExtractorInstances

trait GenericExtractorInstances {

  implicit val hNilExt: ObjExt[HNil] = JsExtractor.obj(MObj.empty)(_ => Extracted(HNil))

  implicit def hlistExt[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    LHExt: Lazy[JsExtractor[H]],
    tExt: ObjExt[T]
  ): ObjExt[FieldType[K, H] :: T] = {
    val hExt = LHExt.value
    val key = witness.value.name
    val headType = witness.value.name -> hExt.`type`
    val `type` = MObj(headType +: tExt.`type`.fields)

    JsExtractor.obj(`type`)(map => {
      val headV = map.fieldValue(key)
      (hExt(headV), tExt(map)) match {
        case (Extracted(h), Extracted(t)) => Extracted((h :: t).asInstanceOf[FieldType[K, H] :: T])
        case (Extracted(h), f: FailedExt) => f
        case (f: FailedExt, Extracted(t)) => f
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
    JsExtractor.obj(ext.`type`)(map => ext(map) match {
      case Extracted(h) => Extracted(labGen.from(h))
      case f: FailedExt => FailedExt.IncompleteObject(clzTag.runtimeClass.getName, f)
    })

}

object GenericExtractorInstances extends GenericExtractorInstances

