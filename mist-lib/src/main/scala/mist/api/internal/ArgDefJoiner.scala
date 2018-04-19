package mist.api.internal

import mist.api.ArgDef
import shadedshapeless._


trait ArgDefJoiner[A] {
  type Out
  def apply(a: A): ArgDef[Out]
}

object ArgDefJoiner {

  type Aux[A, Out0] = ArgDefJoiner[A] { type Out = Out0 }

  trait Reducer[A] {
    type Out
    def apply(a: A): ArgDef[Out]
  }

  trait LowPriorityReducer {
    type Aux[A, Out0] = Reducer[A] { type Out = Out0 }
    implicit def lastReducer[H, HA, T <: HNil](implicit ev: H <:< ArgDef[HA]): Aux[H :: HNil, HA] = {
      new Reducer[H :: HNil] {
        type Out = HA
        def apply(a: H :: HNil): ArgDef[HA] = a.head
      }
    }
  }

  object Reducer extends LowPriorityReducer {
    implicit def hlistReducer[H, T <: HList, HA, Out, R](implicit
      ev: H <:< ArgDef[HA],
      reducer: Reducer.Aux[T, Out],
      cmb: ArgCombiner.Aux[HA, Out, R]
    ): Aux[H :: T, R] = {
      new Reducer[H :: T] {
        type Out = R
        def apply(hl: H :: T): ArgDef[R] = {
          val h = hl.head
          val t = hl.tail
          val rO = reducer(t)
          cmb(h, rO)
        }
      }
    }
  }

  implicit def reduced[A, H <: HList, R](implicit
    hlister: HLister.Aux[A, H],
    reducer: Reducer.Aux[H, R]
  ): Aux[A, R] = {
    new ArgDefJoiner[A] {
      type Out = R
      def apply(a: A): ArgDef[R] = {
        reducer(hlister(a))
      }
    }
  }

  def apply[A, Out](a: A)(implicit joiner: ArgDefJoiner.Aux[A, Out]): ArgDef[Out] = joiner(a)
}

