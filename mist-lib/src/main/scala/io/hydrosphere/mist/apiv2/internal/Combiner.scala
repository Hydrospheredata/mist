package io.hydrosphere.mist.apiv2.internal

import shapeless._

trait Combiner[A, B] extends DepFn2[A, B] with Serializable

trait LowPriorityCombiner {

  implicit def combinerX[H, T]: Combiner[H, T] = new Combiner[H, T] {
    override type Out = H :: T :: HNil

    override def apply(t: H, u: T): Out = t :: u :: HNil
  }
}

object Combiner extends LowPriorityCombiner {
  type Aux[A, B, Out0] = Combiner[A, B] {type Out = Out0}

  def apply[A, B](implicit comb: Combiner[A, B]): Combiner[A, B] = comb

  implicit def combinerHList[H <: HList, T]: Combiner[H, T] = new Combiner[H, T] {
    type Out = T :: H

    override def apply(t: H, u: T): T :: H = u :: t
  }
}

