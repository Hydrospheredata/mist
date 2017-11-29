package mist.api

import mist.api.args._

trait JobDef[A] { self =>

  def invoke(ctx: JobContext): JobResult[A]

  def describe(): Seq[ArgInfo]

  def validate(params: Map[String, Any]): Either[Throwable, Any]

}

object JobDef {

  def instance[A](
    f: JobContext => JobResult[A],
    descr: => Seq[ArgInfo],
    validateF: Map[String, Any] => Either[Throwable, Any]
  ): JobDef[A] = new JobDef[A] {

    override def describe(): Seq[ArgInfo] = descr

    override def invoke(ctx: JobContext): JobResult[A] = {
      try {
        f(ctx)
      } catch {
        case e: Throwable => JobResult.failure(e)
      }
    }

    override def validate(params: Map[String, Any]): Either[Throwable, Any] = validateF(params)
  }
}
