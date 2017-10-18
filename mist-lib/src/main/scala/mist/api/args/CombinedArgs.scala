package mist.api.args

import mist.api._
import shapeless.HList
import shapeless.ops.hlist.IsHCons
import shapeless.ops.tuple.LeftReducer

trait CombinedArgs {
  type Out
  def apply(): ArgDef[Out]
}

//object CombinedArgs {
//  type Aux[Out0] = CombinedArgs { type Out = Out0 }
//
//  implicit def fromProduct[P <: Product, U]
//    (implicit leftFolder: LeftFolder.Aux[]): Aux[toTraversable.Out] = new CombinedArgs {
//
//    override type Out =
//    override def apply(): ArgDef[this.type] =
//  }
//
//}

object TestComb extends App {

  import mist.api.JobDefInstances._
  import shapeless.syntax.std.tuple._
  import shapeless.{Poly1, Poly2}
  //import shapeless._


  object size extends Poly1 {
    implicit def caseArg[A] = at[ArgDef[A]](a => a)
  }
  object addSize extends Poly2 {
    implicit def default[T] = at[String, ArgDef[T]]{(acc: String, arg: ArgDef[T]) => acc + "," + arg.toString}
    implicit def default2[T] = at[ArgDef[T], String]{(arg: ArgDef[T], acc: String ) => acc + "," + arg.toString}
  }
  object ASD extends Poly2 {
    implicit def xx = at[String, String]{(b: String, a: String) => b + a}
  }

  object reducer extends Poly2 {
    implicit def reduce[A, B](implicit cmb: ArgCombiner[A, B]) = at[ArgDef[A], ArgDef[B]] {(a: ArgDef[A], b: ArgDef[B]) =>
      println(s"Acc1: ${a}, t: $b")
      cmb(a, b)
    }
  }

  def withArgs[A, Out](a: A)(
    implicit
      caseB: reducer.Case.Aux[]
      r: LeftReducer.Aux[A, reducer.type, Out],

  ): Out = {
    r(a)
  }

//  val x = (arg[String]("a"), arg[Int]("b"), arg[String]("x"))

  val ctx = JobContext(null, Map("a" -> "A", "b" -> 2, "x" -> "X"))
  val b = withArgs(arg[String]("a"), arg[Int]("b"), arg[String]("x"))
  val c = b & ArgDef.const("CONST")
  println(c.extract(ctx))

//  val a1 = arg[String]("a") & arg[Int]("b")
//  val a2 = arg[String]("x") & a1
//
//  val x2 = (arg[String]("a") & arg[Int]("b") & arg[String]("x"))

//  println((arg[String]("a"),arg[Int]("b"), arg[String]("x")).foldRight("a")(addSize))
//  println(("a", "b", "x").reduceLeft(ASD))
//  val result = x.reduceRight(reducer)

//  println(result.extract(ctx))
//  println(x2.extract(ctx))
//  println(a2.extract(ctx))
  println("HELLO!")
}
