package mist.api.args

import mist.api.ArgDef
import shapeless.Poly2
import shapeless.ops.tuple.LeftReducer

trait WithArgsScala {

//  object reducer extends Poly2 {
//    implicit def reduce[A, B](implicit cmb: ArgCombiner[A, B]): Arg =
//      at[ArgDef[A], ArgDef[B]] {(a: ArgDef[A], b: ArgDef[B]) =>
//      cmb(a, b)
//    }
//  }
//
//  def withArgs[A, Out](a: A)(implicit : LeftReducer.Aux[A, reducer.type, Out]): Out = {
//    r(a)
//  }
}
