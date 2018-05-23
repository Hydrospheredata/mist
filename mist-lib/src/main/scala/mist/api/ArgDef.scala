package mist.api

import mist.api.internal.{ArgCombiner, FnForTuple}
import mist.api.data.JsMap

import scala.util._

trait ArgDef[A] { self =>

  def extract(ctx: FnContext): Extraction[A]
  def describe(): Seq[ArgInfo]
  private[api] def validate(params: JsMap): Extraction[Unit]

  final def combine[B](other: ArgDef[B])(implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)
  final def &[B](other: ArgDef[B])(implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  final def map[B](f: A => B): ArgDef[B] = {
    new ArgDef[B] {
      override def describe(): Seq[ArgInfo] = self.describe()
      override def extract(ctx: FnContext): Extraction[B] = self.extract(ctx).map(f)
      override def validate(params: JsMap): Extraction[Unit] = self.validate(params)
    }
  }

  final def andThen[B](f: A => Extraction[B]): ArgDef[B] = {
    new ArgDef[B] {
      override def describe(): Seq[ArgInfo] = self.describe()
      override def extract(ctx: FnContext): Extraction[B] = self.extract(ctx).flatMap(f)
      override def validate(params: JsMap): Extraction[Unit] = self.validate(params)
    }
  }

  final def apply[F, R, RR <: R](f: F)(implicit fnT: FnForTuple.Aux[A, F, RR]): RawHandle[R] = {
    new RawHandle[R] {
      override def invoke(ctx: FnContext): Try[R] = self.extract(ctx) match {
        case Extracted(a) => Try(fnT(f, a))
        case f: Failed => Failure(new IllegalArgumentException(s"Arguments does not conform to job [$f]"))
      }
      override def describe(): Seq[ArgInfo] = self.describe()
      override def validate(params: JsMap): Extraction[Unit] = self.validate(params)
    }
  }

}
