package mist.api.args

import mist.api.ArgDef
import shapeless.ops.hlist.LeftReducer
import shapeless.{Generic, HList, Poly2}

/**
  * Scala dsl to start job definition like `withArgs(a,b...n).onSparkContext(..`
  * Magnet pattern explanation - http://spray.io/blog/2012-12-13-the-magnet-pattern/
  */
trait WithArgsScala {

  import WithArgsScala._
  /**
    *
    * Converts several argsDef to one
    * Usage examples:
    *  - withArgs(arg[Int]("my-arg"))                    // single arg
    *  - withArgs(arg[Int]("first"), arg[Int]("second")) // tuple of args (scala adapted arguments here work fine)
    */
  def withArgs(argMagnet: ArgMagnet): argMagnet.Out = argMagnet()

}

object WithArgsScala extends WithArgsScala {

  trait ArgMagnet {
    type Out
    def apply(): Out
  }

  object ArgMagnet {
    implicit def toMagnet[A](value: A)(implicit toArgDef: ToArgDef[A]): ArgMagnet {type Out = toArgDef.Out} = {
      new ArgMagnet {
        type Out = toArgDef.Out
        def apply(): toArgDef.Out = toArgDef(value)
      }
    }
  }

  sealed trait ToArgDef[A] {
    type Out
    def apply(a: A): Out
  }

  object ToArgDef {
    implicit def single[A]: ToArgDef[ArgDef[A]] {type Out = ArgDef[A]} =
      new ToArgDef[ArgDef[A]] {
       type Out = ArgDef[A]
       def apply(a: ArgDef[A]): ArgDef[A] = a
    }

    object reducer extends Poly2 {
      implicit def reduce[A, B, Out]
      (implicit cmb: ArgCombiner.Aux[A, B, Out]): Case.Aux[ArgDef[A], ArgDef[B], ArgDef[Out]] = {
        at[ArgDef[A], ArgDef[B]] {(a: ArgDef[A], b: ArgDef[B]) =>
          cmb(a, b)
        }
      }
    }

    implicit def forTuple[A, H <: HList](
      implicit
        gen: Generic.Aux[A, H],
        r: LeftReducer[H, reducer.type]): ToArgDef[A] {type Out = r.Out} = {
      new ToArgDef[A] {
        type Out = r.Out
        def apply(a: A): Out = r(gen.to(a))
      }
    }

  }
}

