package mist.api

import mist.api.args.{ArgCombiner, ToJobDef}

trait ArgExtraction[+A] { self =>
  def map[B](f: A => B): ArgExtraction[B] = self match {
    case Extracted(a) => Extracted(f(a))
    case m@Missing(_) => m.asInstanceOf[ArgExtraction[B]]
  }
  def flatMap[B](f: A => ArgExtraction[B]): ArgExtraction[B] = self match {
    case Extracted(a) => f(a)
    case m@Missing(_) => m.asInstanceOf[ArgExtraction[B]]
  }
}

case class Extracted[+A](value: A) extends ArgExtraction[A]
case class Missing[+A](description: String) extends ArgExtraction[A]

trait ArgDef[A] { self =>

  def describe(): Seq[ArgInfo]

  def extract(ctx: JobContext): ArgExtraction[A]

  def validate(params: Map[String, Any]): Either[Throwable, Any]

  def combine[B](other: ArgDef[B])
      (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def &[B](other: ArgDef[B])
    (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def map[B](f: A => B): ArgDef[B] = {
    new ArgDef[B] {

      override def describe(): Seq[ArgInfo] = self.describe()

      override def extract(ctx: JobContext): ArgExtraction[B] = self.extract(ctx).map(f)

      override def validate(params: Map[String, Any]): Either[Throwable, Any] =
        self.validate(params)

    }
  }

  def validated(f: A => Boolean, reason: String = "Validation failed"): ArgDef[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = self.describe()

      override def extract(ctx: JobContext): ArgExtraction[A] = self.extract(ctx)

      override def validate(params: Map[String, Any]): Either[Throwable, Any] =
        extract(JobContext(params)) match {
          case Extracted(a) => self.validate(params) match {
            case Right(_) => if (f(a)) Right(()) else {
              val descr = if (reason.isEmpty) "" else " :" + reason
              val message = s"Arg was rejected by validation rule" + descr
              Left(new IllegalArgumentException(message))
            }
            case Left(err) => Left(err)
          }
          case Missing(err) => Left(new IllegalArgumentException(err))
        }
    }
  }

  def apply[F, R](f: F)(implicit tjd: ToJobDef.Aux[A, F, R]): JobDef[R] = tjd(self, f)

}

trait UserArg[A] extends ArgDef[A] {
  override def validate(params: Map[String, Any]): Either[Throwable, Any] = extract(JobContext(params)) match {
    case Extracted(_) => Right(())
    case Missing(err) => Left(new IllegalArgumentException(err))
  }
}

trait SystemArg[A] extends ArgDef[A] {
  override def describe(): Seq[ArgInfo] = Seq(InternalArgument)
  override def validate(params: Map[String, Any]): Either[Throwable, Any] =
    Right(())
}

object ArgDef {

  def create[A](f: JobContext => ArgExtraction[A]): ArgDef[A] = new SystemArg[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = f(ctx)
  }

  def createWithFullCtx[A](f: FullJobContext => A): ArgDef[A] = ArgDef.create {
    case c: FullJobContext => Extracted(f(c))
    case ctx => Missing(s"Unknown type of job context ${ctx.getClass.getSimpleName} expected ${FullJobContext.getClass.getSimpleName}")
  }

  def const[A](value: A): ArgDef[A] = create(_ => Extracted(value))

  def missing[A](message: String): ArgDef[A] = create(_ => Missing(message))

}

