package mist.api.args

import mist.api.ArgDef
import shapeless.ops.hlist.LeftReducer
import shapeless.{Generic, HList, Poly2}


/**
  * Scala dsl to start job definition like `withArgs(a,b...n).onSparkContext(..`
  */
trait WithArgsScala {

  import WithArgsScala._

  def withArgs[A, Out](a: A)(implicit tda: ToArgDef.Aux[A, Out]): ArgDef[Out] = tda(a)

}

object WithArgsScala extends WithArgsScala {

  sealed trait ToArgDef[A] {
    type Out
    def apply(a: A): ArgDef[Out]
  }

  object ToArgDef {

    type Aux[A, Out0] = ToArgDef[A] { type Out = Out0 }

    def apply[A](implicit tad: ToArgDef[A]): ToArgDef[A] = tad

    implicit def single[A, X](implicit ev: A <:< ArgDef[X]): Aux[A, X] =
      new ToArgDef[A] {
        type Out = X
        def apply(a: A): ArgDef[X] = a
      }

    object reducer extends Poly2 {
      implicit def reduce[A, B, Out, X, Y]
      (implicit
        ev1: A <:< ArgDef[X],
        ev2: B <:< ArgDef[Y],
        cmb: ArgCombiner.Aux[X, Y, Out]
      ): Case.Aux[A, B, ArgDef[Out]] = {
        at[A, B] {(a: A, b: B) =>
          cmb(a, b)
        }
      }
    }

    implicit def forTuple[A, H <: HList, ROut, Z](
      implicit
      gen: Generic.Aux[A, H],
      r: LeftReducer.Aux[H, reducer.type, ROut],
      ev: ROut <:< ArgDef[Z]
    ): Aux[A, Z] = {
      new ToArgDef[A] {
        type Out = Z
        def apply(a: A): ArgDef[Z] = r(gen.to(a))
      }
    }

  }
}

