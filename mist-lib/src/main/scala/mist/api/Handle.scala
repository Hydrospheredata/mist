package mist.api

import mist.api.args._
import mist.api.data.JsLikeData
import mist.api.encoding.Encoder
import shadedshapeless.HList
import shadedshapeless.ops.function.FnToProduct

trait Handle[A] { self =>

  def invoke(ctx: FnContext): JobResult[A]

  def describe(): Seq[ArgInfo]

  def validate(params: Map[String, Any]): Either[Throwable, Any]

}

object Handle {

  def instance[A](
    f: FnContext => JobResult[A],
    descr: => Seq[ArgInfo],
    validateF: Map[String, Any] => Either[Throwable, Any]
  ): Handle[A] = new Handle[A] {

    override def describe(): Seq[ArgInfo] = descr

    override def invoke(ctx: FnContext): JobResult[A] = {
      try {
        f(ctx)
      } catch {
        case e: Throwable => JobResult.failure(e)
      }
    }

    override def validate(params: Map[String, Any]): Either[Throwable, Any] = validateF(params)
  }
}

trait Handle2[A, Fn] {
  def raw: Fn
  def apply(ctx: FnContext): JobResult[A]
  def inputArguments: Seq[ArgInfo]
}

trait ToHandle2[In, Fn] extends Serializable {
  type Out
  def apply(args: ArgDef[In], f: Fn): Handle2[Out, Fn]
}

object ToHandle2 {

  type Aux[In, F, Out0] = ToHandle2[In, F] { type Out = Out0 }

  implicit def hListEncoded[In <: HList, F, Res](implicit fntp: FnToProduct.Aux[F, In => Res]): Aux[In, F, Res] =
    new ToHandle2[In, F] {
      type Out = Res
      def apply(args: ArgDef[In], f: F): Handle2[Res, F] = {
        new Handle2[Res, F] {
          override def raw: F = f
          override def inputArguments: Seq[ArgInfo] = args.describe()
          override def apply(ctx: FnContext): JobResult[Res] = {
            args.extract(ctx) match {
              case Extracted(a) =>
                val res = fntp(f)(a)
                JobSuccess(res)
              case Missing(err) =>
                val e = new IllegalArgumentException(s"Arguments does not conform to job [$err]")
                JobResult.failure(e)
            }
          }
        }
      }
    }
}

