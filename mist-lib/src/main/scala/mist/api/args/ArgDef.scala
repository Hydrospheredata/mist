package mist.api.args

import mist.api.{FnContext, FullFnContext, Handle, JobFailure, JobSuccess}


sealed trait ArgInfo
case class InternalArgument(tags: Seq[String] = Seq.empty) extends ArgInfo
case class UserInputArgument(name: String, t: ArgType) extends ArgInfo

object ArgInfo {
  val StreamingContextTag = "streaming"
  val SqlContextTag = "sql"
  val HiveContextTag = "hive"
}

trait ArgExtraction[+A] { self =>

  def map[B](f: A => B): ArgExtraction[B] = self match {
    case Extracted(a) => Extracted(f(a))
    case m@Missing(_) => m.asInstanceOf[ArgExtraction[B]]
  }

  def flatMap[B](f: A => ArgExtraction[B]): ArgExtraction[B] = self match {
    case Extracted(a) => f(a)
    case m@Missing(_) => m.asInstanceOf[ArgExtraction[B]]
  }

  def isMissing: Boolean = self match {
    case Extracted(a) => false
    case m@Missing(_) => true
  }

  def isExtracted: Boolean = !isMissing

}

case class Extracted[+A](value: A) extends ArgExtraction[A]
case class Missing[+A](description: String) extends ArgExtraction[A]

trait ArgDef[A] { self =>

  def describe(): Seq[ArgInfo]

  def extract(ctx: FnContext): ArgExtraction[A]

  private[api] def validate(params: Map[String, Any]): Either[Throwable, Any]

  def combine[B](other: ArgDef[B])
      (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def &[B](other: ArgDef[B])
    (implicit cmb: ArgCombiner[A, B]): ArgDef[cmb.Out] = cmb(self, other)

  def map[B](f: A => B): ArgDef[B] = {
    new ArgDef[B] {

      override def describe(): Seq[ArgInfo] = self.describe()

      override def extract(ctx: FnContext): ArgExtraction[B] = self.extract(ctx).map(f)

      override def validate(params: Map[String, Any]): Either[Throwable, Any] = self.validate(params)

    }
  }

  def apply[F, R](f: F)(implicit tjd: ToHandle.Aux[A, F, R]): Handle[R] = tjd(self, f)
  def apply2[F, R](f: F)(implicit fnT: FnForTuple.Aux[A, F, R]): Handle[R] = {
    Handle.instance(
      f = (ctx: FnContext) => {
        self.extract(ctx) match {
          case Extracted(args) =>
            val r = fnT(f, args)
            JobSuccess(r)
          case Missing(msg) => JobFailure(new RuntimeException(msg))
        }
      },
      descr = self.describe(),
      validateF = _ => Left(new RuntimeException("ex"))
    )
  }
}

trait SystemArg[A] extends ArgDef[A] {
  override def validate(params: Map[String, Any]): Either[Throwable, Any] =
    Right(())
}

object SystemArg {

  def apply[A](tags: Seq[String], f: => ArgExtraction[A]): ArgDef[A] = new SystemArg[A] {
    override def extract(ctx: FnContext): ArgExtraction[A] = f

    override def describe() = Seq(InternalArgument(tags))
  }

  def apply[A](tags: Seq[String], f: FullFnContext => ArgExtraction[A]): ArgDef[A] = new SystemArg[A] {
    override def extract(ctx: FnContext): ArgExtraction[A] = ctx match {
      case c: FullFnContext => f(c)
      case _ =>
        val desc = s"Unknown type of job context ${ctx.getClass.getSimpleName} " +
          s"expected ${FullFnContext.getClass.getSimpleName}"
        Missing(desc)
    }

    override def describe() = Seq(InternalArgument(tags))
  }
}

object ArgDef {
  def const[A](value: A): ArgDef[A] = SystemArg(Seq.empty, Extracted(value))

  def missing[A](message: String): ArgDef[A] = SystemArg(Seq.empty, Missing(message))
}

