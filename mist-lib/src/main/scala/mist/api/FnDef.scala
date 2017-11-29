package mist.api

import mist.api.args._

trait FnDef[A] { self =>

  def invoke(ctx: FnContext): JobResult[A]

  def describe(): Seq[ArgInfo]

  def validate(params: Map[String, Any]): Either[Throwable, Any]

}

object FnDef {

  def instance[A](
    f: FnContext => JobResult[A],
    descr: => Seq[ArgInfo],
    validateF: Map[String, Any] => Either[Throwable, Any]
  ): FnDef[A] = new FnDef[A] {

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
