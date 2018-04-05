package mist.api.args

import mist.api._

trait ArgCombiner[A, B] {
  type Out
  def apply(a: ArgDef[A], b: ArgDef[B]): ArgDef[Out]
}

object ArgCombiner {
  type Aux[A, B, Out0] = ArgCombiner[A, B] { type Out = Out0 }

  implicit def combiner[A, B, R](implicit join: JoinToTuple.Aux[A, B, R]): Aux[A, B, R] = new ArgCombiner[A, B] {
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

  def apply[A, B, R](a: ArgDef[A], b: ArgDef[B])(implicit cmb: ArgCombiner.Aux[A, B, R]): ArgDef[R] = cmb(a, b)
}
