package mist.api

import mist.api.data.{JsMap, JsNull}
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

  final override def validate(params: JsMap): Extraction[Unit] =
    extract(FnContext.onlyInput(params)).map(_ => ())
}

class NamedUserArg[A](name: String)(implicit ext: JsExtractor[A]) extends UserArg[A] {
  override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, ext.`type`))
  override def extract(ctx: FnContext): Extraction[A] =
    ext.transformFailure(f => Failed.InvalidField(name, f))(ctx.params.fieldValue(name))

  override def toString: String = s"${getClass.getSimpleName}($name)"
}

class NamedUserArgWithDefault[A](name: String, default: A)(implicit ext: JsExtractor[A]) extends UserArg[A] {
  override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, MOption(ext.`type`)))
  override def extract(ctx: FnContext): Extraction[A] = {
    ctx.params.fieldValue(name) match {
      case JsNull => Extracted(default)
      case x => ext.transformFailure(f => Failed.InvalidField(name, f))(x)
    }
  }
  override def toString: String = s"${getClass.getSimpleName}($name, $default)"
}

