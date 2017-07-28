package io.hydrosphere.mist.api.v2

import scala.annotation.tailrec

/**
  * taken from shapeless
  */
object hlist {

  sealed trait HList extends Product with Serializable {

    def reverse: HList = {
      @tailrec
      def loop(l: HList, suffix: HList): HList = {
        l match {
          case HNil => suffix
          case h :: t => loop(t, hlist.::(h,suffix))
        }
      }
      loop(this, HNil)
    }
  }

  final case class ::[+H, +T <: HList](head: H, tail: T) extends HList {

    override def toString = head match {
      case _: ::[_, _] => "(" + head + ") :: " + tail.toString
      case _ => head + " :: " + tail.toString
    }

    def ::[H](h: H) = hlist.::(h, this)

  }


  sealed trait HNil extends HList {
    def ::[H](h: H) = hlist.::(h, this)

    override def toString = "HNil"
  }

  case object HNil extends HNil

}

