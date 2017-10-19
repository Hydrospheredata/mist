package mist.api.args

import mist.api.ArgDef
import shapeless.{Generic, HList, Poly2}

/**
  * Scala dsl to start job definition like `withArgs(a,b...n).onSparkContext(..`
  * Magnet pattern explanation - http://spray.io/blog/2012-12-13-the-magnet-pattern/
  */
trait WithArgsScala extends WithArgsScalaInternal {

  /**
    *
    * Converts several argsDef to one
    * Usage examples:
    *  - withArgs(arg[Int]("my-arg"))                    // single arg
    *  - withArgs(arg[Int]("first"), arg[Int]("second")) // tuple of args (scala adapted arguments here work fine)
    */
  def withArgs(toArgDef: ToArgMagnet): toArgDef.Out = toArgDef()

}

trait WithArgsScalaInternal {

  object reducer extends Poly2 {
    implicit def reduce[A, B, Out]
    (implicit cmb: ArgCombiner.Aux[A, B, Out]): Case.Aux[ArgDef[A], ArgDef[B], ArgDef[Out]] = {
      at[ArgDef[A], ArgDef[B]] {(a: ArgDef[A], b: ArgDef[B]) =>
        cmb(a, b)
      }
    }
  }

  trait ToArgMagnet {
    type Out
    def apply(): Out
  }

  object ToArgMagnet {
    implicit def apply[A, H <: HList](value: A)(
      implicit
        gen: Generic.Aux[A, H],
        r: shapeless.ops.hlist.LeftReducer[H, reducer.type]): ToArgMagnet {type Out = r.Out} = {
      new ToArgMagnet {
        type Out = r.Out
        def apply(): Out = r(gen.to(value))
      }
    }

    implicit def single[A](value: ArgDef[A]): ToArgMagnet {type Out = ArgDef[A]} = new ToArgMagnet{
      type Out = ArgDef[A]
      def apply(): ArgDef[A] = value
    }
  }
}

object WithArgsScala extends WithArgsScala
