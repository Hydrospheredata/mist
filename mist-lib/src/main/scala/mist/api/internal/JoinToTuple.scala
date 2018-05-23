package mist.api.internal

import shadedshapeless._
import shadedshapeless.ops.hlist.{Prepend, Tupler}

trait JoinToTuple[A, B] {
  type Out
  def apply(a: A, b: B): Out
}

object JoinToTuple {

  type Aux[A, B, Out0] = JoinToTuple[A, B] { type Out = Out0 }

  implicit def join[A, B, H1 <: HList, H2 <: HList, H3 <: HList, R](implicit
    hl1: HLister.Aux[A, H1],
    hl2: HLister.Aux[B, H2],
    prep: Prepend.Aux[H1, H2, H3],
    tupler: Tupler.Aux[H3, R]
  ): Aux[A, B, R] = {
    new JoinToTuple[A, B] {
      type Out = R
      override def apply(a: A, b: B): R = {
        tupler(prep(hl1(a), hl2(b)))
      }
    }
  }

  def apply[A, B, R](a: A, b: B)(implicit join: JoinToTuple.Aux[A, B, R]): R = join(a, b)
}


