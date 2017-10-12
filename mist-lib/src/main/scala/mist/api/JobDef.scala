package mist.api

import mist.api.args.MOption

trait JobDefInstances extends ArgDescriptionInstances {

  class NamedArgDef[A](name: String)(implicit descr: ArgDescription[A]) extends ArgDef[A] {

    override def extract(ctx: JobContext): ArgExtraction[A] = {
      ctx.params.get(name).flatMap(a => descr(a)) match {
        case Some(a) => Extracted(a)
        case None => Missing(s"Argument $name is missing or has incorrect type")
      }
    }

    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, descr.`type`))
  }

  class NamedArgWithDefault[A](name: String, default: A)(implicit descr: ArgDescription[A]) extends ArgDef[A] {
    override def extract(ctx: JobContext): ArgExtraction[A] = {
      ctx.params.get(name) match {
        case Some(any) => descr(any) match {
          case Some(v) => Extracted(v)
          case None => Missing(s"Argument $name is missing or has incorrect type")
        }
        case None => Extracted(default)
      }
    }

    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, descr.`type`))
  }

  class OptionalNamedArgDef[A](name: String)(implicit descr: ArgDescription[A]) extends ArgDef[Option[A]] {
    override def extract(ctx: JobContext): ArgExtraction[Option[A]] = {
      ctx.params.get(name) match {
        case Some(any) =>
          descr(any) match {
            case Some(a) => Extracted(Some(a))
            case None => Missing(s"Argument $name has incorrect type")
          }
        case None => Extracted(None)
      }
    }

    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, MOption(descr.`type`)))
}



  def arg[A](name: String)(implicit a: ArgDescription[A]): ArgDef[A] = new NamedArgDef[A](name)

  def arg[A](name: String, default: A)(implicit a: ArgDescription[A]): ArgDef[A] = new NamedArgWithDefault[A](name, default)

  def optArg[A](name: String)(implicit a: ArgDescription[A]): ArgDef[Option[A]] = new OptionalNamedArgDef[A](name)

  val allArgs: ArgDef[Map[String, Any]] = new ArgDef[Map[String, Any]] {
    override def describe(): Seq[ArgInfo] = Seq(InternalArgument)
    override def extract(ctx: JobContext): ArgExtraction[Map[String, Any]] = Extracted(ctx.params)
  }
}

object JobDefInstances extends JobDefInstances

trait JobDef[A] { self =>

  def invoke(ctx: JobContext): JobResult[A]

  def describe(): Seq[ArgInfo]

  def validate(params: Map[String, Any]): Either[Throwable, Map[String, Any]]

}

object JobDef {

  def instance[A](
    f: JobContext => JobResult[A],
    descr: => Seq[ArgInfo],
    validateF: Map[String, Any] => Either[Throwable, Map[String, Any]]
  ): JobDef[A] = new JobDef[A] {

    override def describe(): Seq[ArgInfo] = descr

    override def invoke(ctx: JobContext): JobResult[A] = {
      try {
        f(ctx)
      } catch {
        case e: Throwable => JobResult.failure(e)
      }
    }

    override def validate(params: Map[String, Any]): Either[Throwable, Map[String, Any]] = validateF(params)
  }
}
