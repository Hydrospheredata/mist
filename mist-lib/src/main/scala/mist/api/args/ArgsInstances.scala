package mist.api.args

import mist.api.JobContext

trait ArgsInstances {

  class NamedUserArg[A](name: String)(implicit pe: PlainExtractor[A]) extends UserArg[A] {
    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, pe.`type`))

    override def extract(ctx: JobContext): ArgExtraction[A] = {
      pe.extract(ctx.params.get(name))
    }
  }

  class NamedUserArgWithDefault[A](name: String, default: A)(implicit pe: PlainExtractor[A]) extends UserArg[A] {
    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, MOption(pe.`type`)))

    override def extract(ctx: JobContext): ArgExtraction[A] = {
      ctx.params.get(name) match {
        case s if s.isDefined => pe.extract(s)
        case none => Extracted(default)
      }
    }
  }

  def arg[A](name: String)(implicit pe: PlainExtractor[A]): UserArg[A] =
    new NamedUserArg[A](name)

  def arg[A](name: String, default: A)(implicit pe: PlainExtractor[A]): UserArg[A] =
    new NamedUserArgWithDefault[A](name, default)

  def arg[A](implicit le: LabeledExtractor[A]): UserArg[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = le.info

      override def extract(ctx: JobContext) = le.extract(ctx.params)
    }
  }

  val allArgs: ArgDef[Map[String, Any]] = new SystemArg[Map[String, Any]] {
    override def extract(ctx: JobContext): ArgExtraction[Map[String, Any]] = Extracted(ctx.params)

    override def describe(): Seq[ArgInfo] = Seq(InternalArgument())
  }
}

object ArgsInstances extends ArgsInstances

