package mist.api.args

import mist.api._
import shapeless.{HList, HNil}
import shapeless.ops.function.FnToProduct

//trait ArgToJobDef[A, F] extends Serializable {
//  type Out
//  def apply(args: ArgDef[A], f: F): JobDef[Out]
//}
//
//object ArgToJobDef {
//
//  type Aux[A, F, Res] = ArgToJobDef[A, F] { type Out = Res }
//
//  implicit def defImpl[A, F, Res, H <: HList]
//    (implicit
//      norm: Normalizer.Aux[A, H],
//      fntp: FnToProduct.Aux[F, H => Res]
//    ) : Aux[A, F, Res]= new ArgToJobDef[A, F] {
//     type Out = Res
//
//    override def apply(a: ArgDef[A], f: F): JobDef[Res] = {
//
//      new JobDef[Res] {
//
//        override def invoke(ctx: JobContext): JobResult[Any]= {
//          a.extract(ctx) match {
//            case Extracted(args) =>
//              try {
//                val result = fntp(f)(norm(args))
//                JobResult.success(result)
//              } catch {
//                case e: Throwable => JobResult.failure(e)
//              }
//            case Missing(msg) =>
//              val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
//              JobResult.failure(e)
//          }
//        }
//
//      }
//    }
//  }
//}

trait ToJobDef[In, F] extends Serializable {
  type Out

  def apply(args: ArgDef[In], f: F): JobDef[Out]
}

object ToJobDef {

  type Aux[In, F, Out0] = ToJobDef[In, F] { type Out = Out0 }

//  implicit def hListEncoded[In <: HList, F, Res](
//    implicit encodedFntp: FnToProductTcResult.Aux[F, Encoder, In => Res, Res]
//  ): Aux[In, F, Res] = new ToJobDef[In, F] {
//    type Out = Res
//    override def apply(args: ArgDef[In], f: F): JobDef[MData] = {
//      JobDef.instance(ctx => {
//        args.extract(ctx) match {
//            case Extracted(a) =>
//              try {
//                val raw = encodedFntp(f)(a)
//                val result = encodedFntp.tc().apply(raw)
//                JobResult.success(result)
//              } catch {
//                case e: Throwable => JobResult.failure(e)
//              }
//            case Missing(msg) =>
//              val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
//              JobResult.failure(e)
//        }
//      })
//    }
//  }

  implicit def hListEncoded[In <: HList, F, Res](
    implicit fntp: FnToProduct.Aux[F, In => Res]
  ): Aux[In, F, Res] = new ToJobDef[In, F] {

      type Out = Res
      def apply(args: ArgDef[In], f: F): JobDef[Res] = {
        JobDef.instance(ctx => {
          args.extract(ctx) match {
            case Extracted(a) =>
              try {
                val result = fntp(f)(a)
                JobResult.success(result)
              } catch {
                case e: Throwable => JobResult.failure(e)
              }
            case Missing(msg) =>
              val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
              JobResult.failure(e)
        }
      })
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
        })
      }
  }
}
