package mist.api

import mist.api.data.JsLikeMap
import mist.api.encoding.JsExtractor

trait UserArg[A] extends ArgDef[A] { self =>

  def validated(f: A => Boolean, reason: String = "Validation failed"): UserArg[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = self.describe()
      override def extract(ctx: FnContext): Extraction[A] =
        self.extract(ctx).flatMap(a => {
          if (f(a)) Extracted(a)
          else {
            val descr = if (reason.isEmpty) "" else " :" + reason
            val message = s"Arg was rejected by validation rule" + descr
            Failed.InvalidValue(message)
          }
        })
    }
  }

  final override def validate(params: JsLikeMap): Extraction[Unit] =
    extract(FnContext(params)).map(_ => ())
}

class NamedUserArg[A](name: String)(implicit ext: JsExtractor[A]) extends UserArg[A] {
  override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, ext.`type`))
  override def extract(ctx: FnContext): Extraction[A] = ext(ctx.params.fieldValue(name))
}

class NamedUserArgWithDefault[A](name: String, default: A)(implicit ext: JsExtractor[A]) extends UserArg[A] {
  private val optExt = ext.orElse(default)
  override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, MOption(ext.`type`)))
  override def extract(ctx: FnContext): Extraction[A] = optExt.apply(ctx.params.fieldValue(name))
}

