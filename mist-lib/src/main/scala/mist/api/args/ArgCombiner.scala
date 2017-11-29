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

        def extract(ctx: FnContext): ArgExtraction[adj.Out] = {
          (b.extract(ctx), a.extract(ctx)) match {
            case (Extracted(be), Extracted(ae)) =>
              val out = adj(ae :: be :: HNil)
              Extracted(out)
            case (Missing(errB), Missing(errA)) => Missing(errA + ", " + errB)
            case (x@Missing(_), _) => x.asInstanceOf[Missing[adj.Out]]
            case (_, x@Missing(_)) => x.asInstanceOf[Missing[adj.Out]]
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

}
