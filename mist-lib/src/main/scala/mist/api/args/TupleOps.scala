package mist.api.args

import mist.api.FnContext
import shadedshapeless.HNil
import shadedshapeless._
import shadedshapeless.ops.adjoin.Adjoin
import shadedshapeless.ops.hlist.{Prepend, Tupler}

trait IsTuple[A]
object IsTuple {
  implicit def one[A]: IsTuple[Tuple1[A]] = null
  implicit def two[A, B]: IsTuple[(A, B)] = null
  implicit def three[A, B, C]: IsTuple[(A, B, C)] = null
  implicit def four[A, B, C, D]: IsTuple[(A, B, C, D)] = null
}

trait HLister[A] {
  type Out
  def apply(a: A): Out
}

trait LowerHLister {

  type Aux[A, Out0] = HLister[A] {type Out = Out0 }

  implicit def toTuple[A]: Aux[A, A :: HNil] = new HLister[A] {
    override type Out = A :: HNil
    override def apply(a: A): A :: HNil = a :: HNil
  }
}

object HLister  extends LowerHLister {

  implicit def one[A]: Aux[Tuple1[A], A :: HNil] = new HLister[Tuple1[A]] {
    override type Out = A :: HNil
    override def apply(a: Tuple1[A]): A :: HNil = a._1 :: HNil
  }

  implicit def two[A, B]: Aux[(A, B), A :: B :: HNil] = new HLister[(A, B)] {
    override type Out = A :: B :: HNil
    override def apply(a: (A, B)): A :: B :: HNil = a._1 :: a._2 :: HNil
  }

  implicit def three[A, B, C]: Aux[(A, B, C), A :: B :: C :: HNil] = new HLister[(A, B, C)] {
    override type Out = A :: B :: C :: HNil
    override def apply(a: (A, B, C)): A :: B :: C :: HNil = a._1 :: a._2 :: a._3 :: HNil
  }

  implicit def four[A, B, C, D]: Aux[(A, B, C, D), A :: B :: C :: D :: HNil] = new HLister[(A, B, C, D)] {
    override type Out = A :: B :: C :: D ::HNil
    override def apply(a: (A, B, C, D)): A :: B :: C :: D :: HNil = a._1 :: a._2 :: a._3 :: a._4 :: HNil
  }

  implicit def five[A, B, C, D, E]: Aux[(A, B, C, D, E), A :: B :: C :: D :: E :: HNil] = new HLister[(A, B, C, D, E)] {
    override type Out = A :: B :: C :: D :: E :: HNil
    override def apply(a: (A, B, C, D, E)): A :: B :: C :: D :: E :: HNil = a._1 :: a._2 :: a._3 :: a._4 :: a._5 :: HNil
  }
}

trait TupleJoin[A, B] {
  type Out
  def apply(a: A, b: B): Out
}

object TupleJoin {

  type Aux[A, B, Out0] = TupleJoin[A, B] { type Out = Out0 }

  implicit def join[A, B, H1 <: HList, H2 <: HList, H3 <: HList, R](implicit
    hl1: HLister.Aux[A, H1],
    hl2: HLister.Aux[B, H2],
    prep: Prepend.Aux[H1, H2, H3],
    tupler: Tupler.Aux[H3, R]
  ): Aux[A, B, R] = {
    new TupleJoin[A, B] {
      type Out = R
      override def apply(a: A, b: B): R = {
        tupler(prep(hl1(a), hl2(b)))
      }
    }
  }

  def apply[A, B, R](a: A, b: B)(implicit join: TupleJoin.Aux[A, B, R]): R = join(a, b)
}

trait Combiner[A, B] {
  type Out
  def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out]
}

object Combiner {
  type Aux[A, B, Out0] = Combiner[A, B] { type Out = Out0 }

  implicit def combiner[A, B, R](implicit join: TupleJoin.Aux[A, B, R]): Aux[A, B, R] = new Combiner[A, B] {
    type Out = R

    override def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out] = {
      new ArgDef[R] {
        def describe(): Seq[ArgInfo] = a.describe() ++ b.describe()

        def extract(ctx: FnContext): ArgExtraction[R] = {
          (b.extract(ctx), a.extract(ctx)) match {
            case (Extracted(be), Extracted(ae)) =>
              val out = join(ae, be)
              Extracted(out)
            case (Missing(errB), Missing(errA)) => Missing(errA + ", " + errB)
            case (x@Missing(_), _) => x.asInstanceOf[Missing[R]]
            case (_, x@Missing(_)) => x.asInstanceOf[Missing[R]]
          }
        }

        override def validate(params: Map[String, Any]): Either[Throwable, Any] = {
          (a.validate(params), b.validate(params)) match {
            case (Right(_), Right(_)) => Right(())
            case (Left(errA), Left(errB)) =>
              Left(new IllegalArgumentException(errA.getLocalizedMessage + "\n" + errB.getLocalizedMessage))
            case (Left(errA), Right(_)) => Left(errA)
            case (Right(_), Left(errB)) => Left(errB)
          }
        }
      }
    }
  }

  def apply[A, B, R](a: ArgDef[A], b: ArgDef[B])(implicit cmb: Combiner.Aux[A, B, R]): ArgDef[R] = cmb(a, b)
}

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
      cmb: Combiner.Aux[HA, Out, R]
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

//  implicit def single[A, X](implicit ev: A <:< ArgDef[X]): Aux[A, X] = {
//    new ArgDefJoiner[A] {
//      type Out = X
//      def apply(a: A): ArgDef[X] = a
//    }
//  }

  def apply[A, Out](a: A)(implicit joiner: ArgDefJoiner.Aux[A, Out]): ArgDef[Out] = joiner(a)
}

trait WithArsScala2 {

  def withArs2[A, Out](a: A)(implicit joiner: ArgDefJoiner.Aux[A, Out]): ArgDef[Out] = joiner(a)

}

object WithArsScala2 extends WithArsScala2

trait FnForTuple[In, F] {
  type Out
  def apply(f: F, in: In): Out
}

object FnForTuple {
  type Aux[In, F, Out0] = FnForTuple[In, F] { type Out = Out0 }

  implicit def one[A, R]: Aux[A, A => R, R] = new FnForTuple[A, A => R] {
    type Out = R
    def apply(f: A => R, in: A): R = f(in)
  }

  implicit def two[A, B, R]: Aux[(A, B), (A, B) => R, R] = new FnForTuple[(A, B), (A, B) => R] {
    type Out = R
    def apply(f: (A, B) => R, in: (A, B)): R = f.tupled(in)
  }

  implicit def three[A, B, C, R]: Aux[(A, B, C), (A, B, C) => R, R] = new FnForTuple[(A, B, C), (A, B, C) => R] {
    type Out = R
    def apply(f: (A, B, C) => R, in: (A, B, C)): R = f.tupled(in)
  }

  implicit def four[A, B, C, D, R]: Aux[(A, B, C, D), (A, B, C, D) => R, R] = new FnForTuple[(A, B, C, D), (A, B, C, D) => R] {
    type Out = R
    def apply(f: (A, B, C, D) => R, in: (A, B, C, D)): R = f.tupled(in)
  }

  implicit def five[A, B, C, D, E, R]: Aux[(A, B, C, D, E), (A, B, C, D, E) => R, R] = new FnForTuple[(A, B, C, D, E), (A, B, C, D, E) => R] {
    type Out = R
    def apply(f: (A, B, C, D, E) => R, in: (A, B, C, D, E)): R = f.tupled(in)
  }
}

