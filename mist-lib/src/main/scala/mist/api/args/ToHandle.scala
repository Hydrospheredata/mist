package mist.api.args

import mist.api._
import shadedshapeless.HList
import shadedshapeless.ops.function.FnToProduct

//trait ToHandle[In, F] extends Serializable {
//  type Out
//
//  def apply(args: ArgDef[In], f: F): Handle[Out]
//}
//
//trait LowLevel {
//
//  type Aux[In, F, Out0] = ToHandle[In, F] { type Out = Out0 }
//  implicit def oneArgEncoded[In, HIn, F, Res](
//    implicit
//    norm: Normalizer.Aux[In, HIn],
//    fntp: FnToProduct.Aux[F, HIn => Res]): Aux[In, F, Res] = new ToHandle[In, F] {
//    type Out = Res
//    def apply(args: ArgDef[In], f: F): Handle[Res] = {
//      Handle.instance(ctx => args.extract(ctx) match {
//        case Extracted(a) =>
//          val result = fntp(f)(norm(a))
//          JobResult.success(result)
//        case Missing(msg) =>
//          val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
//          JobResult.failure(e)
//      }, args.describe(), args.validate)
//    }
//  }
//}
//
//object ToHandle extends LowLevel {
//
//  implicit def hListEncoded[In <: HList, F, Res](
//    implicit fntp: FnToProduct.Aux[F, In => Res]
//  ): Aux[In, F, Res] = new ToHandle[In, F] {
//
//      type Out = Res
//      def apply(args: ArgDef[In], f: F): Handle[Res] = {
//        Handle.instance(ctx => args.extract(ctx) match {
//          case Extracted(a) =>
//            val result = fntp(f)(a)
//            JobResult.success(result)
//          case Missing(msg) =>
//            val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
//            JobResult.failure(e)
//        }, args.describe(), args.validate)
//      }
//  }
//
////  implicit def tupleEncoded[T, In <: HList, F, Res, G](implicit
//////    ev: G <:< In => Res,
////    hlister: HLister.Aux[T, In],
////    fntp: FnToProduct.Aux[F, In => Res]
////  ): Aux[In, F, Res] = new ToHandle[In, F] {
////
////    type Out = Res
////    def apply(args: ArgDef[In], f: F): Handle[Res] = {
////      Handle.instance(ctx => args.extract(ctx) match {
////        case Extracted(a) =>
////          val result = fntp(f)(hlister(a))
////          JobResult.success(result)
////        case Missing(msg) =>
////          val e = new IllegalArgumentException(s"Arguments does not conform to job [$msg]")
////          JobResult.failure(e)
////      }, args.describe(), args.validate)
////    }
////  }
//
//}
