package mist.api
import shapeless.ops.function.FnToProduct

import scala.annotation.implicitNotFound
import scala.language.higherKinds

trait FnToProductTcResult[F, TC[_]] extends Serializable {
  type Out
  type ROut
  def apply(a: F): Out
  def tc(): TC[ROut]
}

object FnToProductTcResult {

  @implicitNotFound(msg = "Could not find instance of ${TC} for resulting value of function ${F}")
  type Aux[F, TC[_], Out0, Out1] = FnToProductTcResult[F, TC] { type Out = Out0 ; type ROut = Out1}

  implicit def instance[F, H, R, TC[_]](implicit
    fntp: FnToProduct.Aux[F, H => R], ev: TC[R] ): Aux[F, TC, H => R, R] =
    new FnToProductTcResult[F, TC] {
      type Out = H => ROut
      type ROut = R
      def apply(a: F): Out = fntp(a)
      def tc(): TC[ROut] = ev
  }
}

