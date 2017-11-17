package mist.api.args

import mist.api.{ArgDef, UserArg}
import shapeless.ops.hlist.{LeftReducer, SubtypeUnifier}
import shapeless.{Generic, HList, Lub, Poly2}

import scala.annotation.implicitNotFound

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

  sealed trait ToArgDef[-A] {
    type Out
    def apply(a: A): Out
  }

  /**
    * Tricks:
    * We need to allow extending ArgDef :
    * class MyArg[A] extends ArgDef[A] { ....
    *
    * But there is problem with implicits - I couldn't find a way how to define ArgCombiner
    * thats supports ArgDef in covariant way
    *
    * Instances:
    * - single - bound an argument to ArgDef[]
    * - forTuple - reduce function uses `single` conversion to refine that arguments are ArgDef's
    */
  object ToArgDef {

    type Aux[-A, Out0] = ToArgDef[A] { type Out = Out0 }

    def apply[A](implicit tad: ToArgDef[A]): ToArgDef[A] = tad

    implicit def single[A]: Aux[ArgDef[A], ArgDef[A]] =
      new ToArgDef[ArgDef[A]] {
        type Out = ArgDef[A]
        def apply(a: ArgDef[A]): ArgDef[A] = a
      }

    object reducer extends Poly2 {
      implicit def reduce[A, B, Out, X, Y]
      (implicit
        toArg1: ToArgDef.Aux[A, ArgDef[X]],
        toArg2: ToArgDef.Aux[B, ArgDef[Y]],
        cmb: ArgCombiner.Aux[X, Y, Out]
      ): Case.Aux[A, B, ArgDef[Out]] = {
        at[A, B] {(a: A, b: B) =>
          cmb(toArg1(a), toArg2(b))
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

