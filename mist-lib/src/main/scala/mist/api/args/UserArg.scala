package mist.api.args

import mist.api.JobContext

trait UserArg[A] extends ArgDef[A] { self=>

  def validated(f: A => Boolean, reason: String = "Validation failed"): UserArg[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = self.describe()

      override def extract(ctx: JobContext): ArgExtraction[A] =
        self.extract(ctx).flatMap(a => {
          if (f(a)) Extracted(a)
          else {
            val descr = if (reason.isEmpty) "" else " :" + reason
            val message = s"Arg was rejected by validation rule" + descr
            Missing(message)
          }
        })

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

  override private[mist] def validate(params: Map[String, Any]): Either[Throwable, Any] =
    extract(JobContext(params)) match {
      case Extracted(_) => Right(())
      case Missing(err) => Left(new IllegalArgumentException(err))
    }

}

