package mist.api.args

import mist.api._
import shapeless.HList
import shapeless.ops.function.FnToProduct

trait ToJobDef[In, F] extends Serializable {
  type Out

  def apply(args: ArgDef[In], f: F): JobDef[Out]
}

object ToJobDef {

  type Aux[In, F, Out0] = ToJobDef[In, F] { type Out = Out0 }

  //TODO: how to validate params without full ctx??
  implicit def hListEncoded[In <: HList, F, Res](
    implicit fntp: FnToProduct.Aux[F, In => Res]
  ): Aux[In, F, Res] = new ToJobDef[In, F] {

      type Out = Res
      def apply(args: ArgDef[In], f: F): JobDef[Res] = {
        JobDef.instance(ctx => args.extract(ctx) match {
          case Extracted(a) =>
            val result = fntp(f)(a)
            JobResult.success(result)
          case Missing(msg) =>
            val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
            JobResult.failure(e)
        }, args.describe(),
          (params: Map[String, Any]) => Right(params)
        )
      }
  }

  implicit def oneArgEncoded[In, HIn, F, Res](
    implicit
    norm: Normalizer.Aux[In, HIn],
    fntp: FnToProduct.Aux[F, HIn => Res]): Aux[In, F, Res] = new ToJobDef[In, F] {
      type Out = Res
      def apply(args: ArgDef[In], f: F): JobDef[Res] = {
        JobDef.instance(ctx => args.extract(ctx) match {
          case Extracted(a) =>
              val result = fntp(f)(norm(a))
              JobResult.success(result)
          case Missing(msg) =>
              val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
              JobResult.failure(e)
        }, args.describe(),
          (params: Map[String, Any]) => Right(params)
        )
      }
  }
}
