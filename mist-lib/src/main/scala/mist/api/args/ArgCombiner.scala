package mist.api.args

import mist.api._
import mist.api.data.JsLikeMap
import mist.api.encoding.{Extracted, Extraction, FailedExt}

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
            case (f1: FailedExt, f2: FailedExt) => FailedExt.toComplex(f1, f2)
            case (f: FailedExt, _) => f
            case (_, f: FailedExt) => f
          }
        }

        //TODO
        override def validate(params: JsLikeMap): Option[Throwable] = {
          (a.validate(params), b.validate(params)) match {
            case (None, None) => None
            case (Some(err1), Some(err2)) => Some(new IllegalArgumentException(s"Vaildation .. $err1 $err2"))
            case (None, err @ Some(_)) => err
            case (err @ Some(_), None) => err
          }
        }
      }
    }
  }

  def apply[A, B, R](a: ArgDef[A], b: ArgDef[B])(implicit cmb: ArgCombiner.Aux[A, B, R]): ArgDef[R] = cmb(a, b)
}
