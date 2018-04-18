package mist.api.args

import mist.api.FnContext
import mist.api.data.JsLikeMap
import mist.api.encoding.{Extracted, Extraction, FailedExt}

trait UserArg[A] extends ArgDef[A] { self=>

  def validated(f: A => Boolean, reason: String = "Validation failed"): UserArg[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = self.describe()
      override def extract(ctx: FnContext): Extraction[A] =
        self.extract(ctx).flatMap(a => {
          if (f(a)) Extracted(a)
          else {
            val descr = if (reason.isEmpty) "" else " :" + reason
            val message = s"Arg was rejected by validation rule" + descr
            FailedExt.InvalidValue(message)
          }
        })
    }
  }

  final override def validate(params: JsLikeMap): Option[Throwable] =
    extract(FnContext(params)) match {
      case Extracted(_) => None
      case f: FailedExt => Some(new IllegalArgumentException(s"Validation failed: $f"))
    }
}

