package mist.api.args

import mist.api._
import shapeless._
import shapeless.ops.adjoin.Adjoin

trait ArgCombiner[A, B] extends Serializable {
  type Out
  def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out]
}

object ArgCombiner {

  type Aux[A, B, Out0] = ArgCombiner[A, B] { type Out = Out0 }

  implicit def combiner[A, B](implicit adj: Adjoin[A :: B :: HNil]): Aux[A, B, adj.Out] = new ArgCombiner[A, B] {
    type Out = adj.Out

    override def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out] = {
      new ArgDef[adj.Out] {
        def describe(): Seq[ArgInfo] = a.describe() ++ b.describe()

        def extract(ctx: JobContext): ArgExtraction[adj.Out] = {
          (b.extract(ctx), a.extract(ctx)) match {
            case (Extracted(be), Extracted(ae)) =>
              val out = adj(ae :: be :: HNil)
              Extracted(out)
            case (Missing(errB), Missing(errA)) => Missing(errA + ", " + errB)
            case (x @ Missing(err1), _ ) => x.asInstanceOf[Missing[adj.Out]]
            case (_, x @ Missing(err2)) => x.asInstanceOf[Missing[adj.Out]]
          }
        }
        override def toString: String = s"From $a and $b"
     }
    }
  }

}
