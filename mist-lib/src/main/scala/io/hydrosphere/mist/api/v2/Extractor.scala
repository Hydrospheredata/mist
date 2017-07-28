package io.hydrosphere.mist.api.v2

import io.hydrosphere.mist.api.v2.hlist.{HList, HNil}

import scala.reflect.runtime.universe._

trait Extractor[T] {
  type In
  type Out

  def fromMap(arg: Arg[T], map: Map[String, Any]): Option[Out]

}

object Extractor {

  trait LowExtractor[T] {
    def fromAny(a: Any): Option[T]
  }

  implicit val strLowExt = new LowExtractor[String] {
    override def fromAny(a: Any): Option[String] = a  match {
        case s: String => Some(s)
        case _ => None
    }
  }

  implicit val intLowExt = new LowExtractor[Int] {
    override def fromAny(a: Any): Option[Int] = a  match {
      case i: Int => Some(i)
      case _ => None
    }
  }

  def printValues[T <: HList](xxxa: hlist.::[Arg[_], T], map: Map[String, Any]): Unit = {
    val arg = xxxa.head
    val v = map.get(arg.name)
    println(s"value for ${arg.name} is $v")
    xxxa.tail match {
      case hlist.::(h: Arg[_], t) =>
        printValues(hlist.::(h, t), map)
      case HNil =>
    }
  }

}
