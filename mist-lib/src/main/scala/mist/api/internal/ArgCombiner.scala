package mist.api.internal

import mist.api._
import mist.api.data.JsMap

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

        def extract(ctx: FnContext): Extraction[R] = {
          (b.extract(ctx), a.extract(ctx)) match {
            case (Extracted(be), Extracted(ae)) =>
              val out = join(ae, be)
              Extracted(out)
            case (f1: Failed, f2: Failed) => Failed.toComplex(f1, f2)
            case (f: Failed, _) => f
            case (_, f: Failed) => f
          }
        }

        override def validate(params: JsMap): Extraction[Unit] = {
          (a.validate(params), b.validate(params)) match {
            case (Extracted(_), Extracted(_)) => Extracted(())
            case (f1: Failed, f2: Failed) => Failed.toComplex(f1, f2)
            case (Extracted(_), f: Failed) => f
            case (f: Failed, Extracted(_)) => f
          }
        }
      }
    }
  }

  def apply[A, B, R](a: ArgDef[A], b: ArgDef[B])(implicit cmb: ArgCombiner.Aux[A, B, R]): ArgDef[R] = cmb(a, b)
}
