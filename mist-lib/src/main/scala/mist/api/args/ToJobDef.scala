package mist.api.args

import mist.api._
import shapeless.HList
import shapeless.ops.function.FnToProduct

trait ToJobDef[In, F] extends Serializable {
  type Out

  def apply(args: ArgDef[In], f: F, tags: Seq[String] = Seq.empty): JobDef[Out]
}

object ToJobDef {

  type Aux[In, F, Out0] = ToJobDef[In, F] { type Out = Out0 }

  implicit def hListEncoded[In <: HList, F, Res](
    implicit fntp: FnToProduct.Aux[F, In => Res]
  ): Aux[In, F, Res] = new ToJobDef[In, F] {

      type Out = Res
      def apply(args: ArgDef[In], f: F, tags: Seq[String]): JobDef[Res] = {
        JobDef.instance(ctx => args.extract(ctx) match {
          case Extracted(a) =>
            val result = fntp(f)(a)
            JobResult.success(result)
          case Missing(msg) =>
            val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
            JobResult.failure(e)
        }, args.describe(), args.validate, tags)
      }
  }

  implicit def oneArgEncoded[In, HIn, F, Res](
    implicit
    norm: Normalizer.Aux[In, HIn],
    fntp: FnToProduct.Aux[F, HIn => Res]): Aux[In, F, Res] = new ToJobDef[In, F] {
      type Out = Res
      def apply(args: ArgDef[In], f: F, tags: Seq[String]): JobDef[Res] = {
        JobDef.instance(ctx => args.extract(ctx) match {
          case Extracted(a) =>
              val result = fntp(f)(norm(a))
              JobResult.success(result)
          case Missing(msg) =>
              val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
              JobResult.failure(e)
        }, args.describe(), args.validate, tags)
      }
  }
}
