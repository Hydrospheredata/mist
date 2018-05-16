package mist.api.encoding

import mist.api._
import mist.api.data._

import scala.annotation.implicitNotFound
import scala.reflect.ClassTag

trait JsExtractor[A] { self =>

  def `type`: ArgType

  def apply(js: JsData): Extraction[A]

  final def map[B](f: A => B): JsExtractor[B] = new JsExtractor[B] {
    def apply(js: JsData): Extraction[B] = self(js).map(f)
    def `type`: ArgType = self.`type`
  }

  final def flatMap[B](f: A => JsExtractor[B]) = new JsExtractor[B] {
    def apply(js: JsData): Extraction[B] = self(js) match {
      case Extracted(a) => f(a)(js)
      case f: Failed => f.asInstanceOf[Extraction[B]]
    }
    def `type`: ArgType = self.`type`
  }

  final def andThen[B](f: A => Extraction[B]) = new JsExtractor[B] {
    def apply(js: JsData): Extraction[B] = self(js).flatMap(f)
    def `type`: ArgType = self.`type`
  }

  final def recover(f: Failed => A): JsExtractor[A] = new JsExtractor[A] {
    def apply(js: JsData): Extraction[A] = self(js) match {
      case failed:Failed => Extracted(f(failed))
      case ext => ext
    }
    def `type`: ArgType = self.`type`
  }

  final def orElse[AA <: A](f: => AA): JsExtractor[AA] = new JsExtractor[AA] {
    def apply(js: JsData): Extraction[AA] = self(js) match {
      case ext if ext.isFailed => Extracted(f)
      case ext => ext.asInstanceOf[Extraction[AA]]
    }
    def `type`: ArgType = self.`type`
  }

  final def transformFailure(f: Failed => Failed): JsExtractor[A] = new JsExtractor[A] {
    override def apply(js: JsData): Extraction[A] = self(js) match {
      case ext: Extracted[_] => ext
      case fail: Failed => f(fail)
    }
    override def `type`: ArgType = self.`type`
  }

}

object JsExtractor {

  def plain[A](argType: ArgType)(f: JsData => Extraction[A]): JsExtractor[A] = new JsExtractor[A] {
    def apply(js: JsData): Extraction[A] = f(js)
    val `type`: ArgType = argType
  }

}

trait defaultExtractors {

  import java.{lang => jl, util => ju}

  implicit val boolExt: JsExtractor[Boolean] = JsExtractor.plain(MBoolean){
    case JsFalse => Extracted(false)
    case JsTrue => Extracted(true)
    case oth => Failed.invalidType("Boolean", oth.toString)
  }
  implicit val jBoolExt: JsExtractor[jl.Boolean] = boolExt.map(b => b)

  implicit val intExt: JsExtractor[Int] = JsExtractor.plain(MInt) {
    case n: JsNumber => Extraction.tryExtract(n.v.toIntExact)(_ => Failed.invalidType("Int", n.v.toString()))
    case oth => Failed.invalidType("Int", oth.toString)
  }

  implicit val jlIntExt: JsExtractor[jl.Integer] = intExt.map(i => i)

  implicit val longExt: JsExtractor[Long] = JsExtractor.plain(MInt) {
    case n: JsNumber => Extraction.tryExtract(n.v.toLongExact)(_ => Failed.invalidType("Long", n.v.toString()))
    case oth => Failed.invalidType("Long", oth.toString)
  }
  implicit val jLongExt: JsExtractor[jl.Long] = longExt.map(l => l)

  implicit val floatExt: JsExtractor[Float] = JsExtractor.plain(MDouble) {
    case n: JsNumber => Extracted(n.v.toFloat)
    case oth => Failed.invalidType("Float", oth.toString)
  }
  implicit val jfloatExt: JsExtractor[jl.Float] = floatExt.map(f => f)

  implicit val doubleExt: JsExtractor[Double] = JsExtractor.plain(MDouble) {
    case n: JsNumber => Extracted(n.v.toDouble)
    case oth => Failed.invalidType("Double", oth.toString)
  }
  implicit val jDoubleExt: JsExtractor[jl.Double] = doubleExt.map(d => d)

  implicit val stringExt: JsExtractor[String] = JsExtractor.plain(MString) {
    case s: JsString => Extracted(s.value)
    case oth => Failed.invalidType("String", oth.toString)
  }

  implicit def optExt[A](implicit ext: JsExtractor[A]): JsExtractor[Option[A]] = JsExtractor.plain(MOption(ext.`type`)) {
    case JsNull => Extracted(None)
    case value => ext(value).map(Some(_))
  }

  implicit def seqExt[A](implicit ext: JsExtractor[A]): JsExtractor[Seq[A]] = JsExtractor.plain(MList(ext.`type`)) {
    case jsList: JsList =>
      val elems = jsList.list.map(ext.apply)
      if (elems.exists(_.isFailed))
        Failed.invalidType(s"List[${ext.`type`}]", jsList.toString)
      else
        Extracted(elems.collect({case Extracted(v) => v}))
    case value => Failed.invalidType(s"List[${ext.`type`}]", value.toString)
  }

  implicit def listExt[A](implicit ext: JsExtractor[Seq[A]]): JsExtractor[List[A]] = ext.map(_.toList)
  implicit def arrExt[A](implicit ext: JsExtractor[Seq[A]], ct: ClassTag[A]): JsExtractor[Array[A]] = ext.map(_.toArray)

  implicit def mapExt[A](implicit ext: JsExtractor[A]): JsExtractor[Map[String, A]] = JsExtractor.plain(MMap(MString, ext.`type`)) {
    case jsMap: JsMap =>
      val elems = jsMap.fields.map({case (k, v) => ext(v).map(e => (k, e))})
      if (elems.exists(_.isFailed))
        Failed.invalidType(s"Map[String, ${ext.`type`}]", jsMap.toString)
      else
        Extracted(elems.collect({case Extracted(v) => v}).toMap)

    case value => Failed.invalidType(s"Map[String, ${ext.`type`}]", value.toString)
  }
}


object defaultExtractors extends defaultExtractors
