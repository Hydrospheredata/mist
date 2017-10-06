package mist.api

import shapeless._

trait Normalizer[A] extends DepFn1[A] with Serializable

trait LowPriorityNormalizer {

  type Aux[A, Out0] = Normalizer[A] { type Out = Out0 }

  implicit def nonHlist[A]: Aux[A, A :: HNil] = new Normalizer[A] {
    type Out = A :: HNil
    override def apply(a: A): A :: HNil = a :: HNil
  }
}

object Normalizer extends LowPriorityNormalizer {


  def apply[A](implicit n: Normalizer[A]): Aux[A, n.Out] = n

  implicit def hnilNormalizer[H <: HNil]: Aux[HNil, HNil] = new Normalizer[HNil] {
    type Out = HNil
    override def apply(h: HNil):HNil = HNil
  }

  implicit def hlistNormalizer[H, T <: HList]: Aux[H :: T, H :: T] = new Normalizer[H :: T] {
    type Out = H :: T
    override def apply(hlist: H :: T): H :: T = hlist
  }


}
