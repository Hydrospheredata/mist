package mist.api.args

import mist.api.data.JsLikeMap
import mist.api.encoding.FailedExt.InvalidType
import mist.api.encoding.{Extracted, Extraction, FailedExt}
import mist.api.{FnContext, FullFnContext, LowHandle}

import scala.util._

sealed trait ArgInfo
case class InternalArgument(tags: Seq[String] = Seq.empty) extends ArgInfo
//TODO optional default value???
case class UserInputArgument(name: String, t: ArgType) extends ArgInfo

object ArgInfo {
  val StreamingContextTag = "streaming"
  val SqlContextTag = "sql"
  val HiveContextTag = "hive"
}

//trait ArgExtraction[+A] { self =>
//
//  def map[B](f: A => B): ArgExtraction[B] = self match {
//    case Extracted(a) => Extracted(f(a))
//    case m@Missing(_) => m.asInstanceOf[ArgExtraction[B]]
//  }
//
//  def flatMap[B](f: A => ArgExtraction[B]): ArgExtraction[B] = self match {
//    case Extracted(a) => f(a)
//    case m@Missing(_) => m.asInstanceOf[ArgExtraction[B]]
//  }
//
//  def isMissing: Boolean = self match {
//    case Extracted(a) => false
//    case m@Missing(_) => true
//  }
//
//  def isExtracted: Boolean = !isMissing
//
//}
//
//case class Extracted[+A](value: A) extends ArgExtraction[A]
//case class Missing[+A](description: String) extends ArgExtraction[A]

trait ArgDef[A] { self =>

  def describe(): Seq[ArgInfo]

  def extract(ctx: FnContext): Extraction[A]

  private[api] def validate(params: JsLikeMap): Option[Throwable]

  def combine[B](other: ArgDef[B])(implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def &[B](other: ArgDef[B])(implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def map[B](f: A => B): ArgDef[B] = {
    new ArgDef[B] {
      override def describe(): Seq[ArgInfo] = self.describe()
      override def extract(ctx: FnContext): Extraction[B] = self.extract(ctx).map(f)
      override def validate(params: JsLikeMap): Option[Throwable] = self.validate(params)
    }
  }

  def apply[F, R](f: F)(implicit fnT: FnForTuple.Aux[A, F, R]): LowHandle[R] = {
    new LowHandle[R] {
      override def invoke(ctx: FnContext): Try[R] = self.extract(ctx) match {
        case Extracted(a) => Try(fnT(f, a))
        case f: FailedExt => Failure(new IllegalArgumentException(s"Arguments does not conform to job [$f]"))
      }
      override def describe(): Seq[ArgInfo] = self.describe()
      override def validate(params: JsLikeMap): Option[Throwable] = self.validate(params)
    }
  }
}

trait SystemArg[A] extends ArgDef[A] {
  override def validate(params: JsLikeMap): Option[Throwable] = None
}

object SystemArg {

  def apply[A](tags: Seq[String], f: => Extraction[A]): ArgDef[A] = new SystemArg[A] {
    override def extract(ctx: FnContext): Extraction[A] = f

    override def describe() = Seq(InternalArgument(tags))
  }

  def apply[A](tags: Seq[String], f: FullFnContext => Extraction[A]): ArgDef[A] = new SystemArg[A] {
    override def extract(ctx: FnContext): Extraction[A] = ctx match {
      case c: FullFnContext => f(c)
      case _ =>
        val desc = s"Unknown type of job context ${ctx.getClass.getSimpleName} " +
          s"expected ${FullFnContext.getClass.getSimpleName}"
        FailedExt.InternalError(desc)
    }

    override def describe() = Seq(InternalArgument(tags))
  }
}

object ArgDef {
  def const[A](value: A): ArgDef[A] = SystemArg(Seq.empty, Extracted(value))

  //TODO?
  def missing[A](message: String): ArgDef[A] = SystemArg(Seq.empty, InvalidType(message, "no value"))
}

