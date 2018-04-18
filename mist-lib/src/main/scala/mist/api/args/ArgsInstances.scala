package mist.api.args

import mist.api.FnContext
import mist.api.encoding.{Extraction, JsExtractor, ObjExt}

trait ArgsInstances {

  class NamedUserArg[A](name: String)(implicit ext: JsExtractor[A]) extends UserArg[A] {
    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, ext.`type`))
    override def extract(ctx: FnContext): Extraction[A] = ext(ctx.params.fieldValue(name))
  }

  class NamedUserArgWithDefault[A](name: String, default: A)(implicit ext: JsExtractor[A]) extends UserArg[A] {
    private val optExt = ext.orElse(default)
    override def describe(): Seq[ArgInfo] = Seq(UserInputArgument(name, MOption(ext.`type`)))
    override def extract(ctx: FnContext): Extraction[A] = optExt.apply(ctx.params.fieldValue(name))
  }

  def arg[A](name: String)(implicit ext: JsExtractor[A]): UserArg[A] = new NamedUserArg[A](name)
  def arg[A](name: String, default: A)(implicit ext: JsExtractor[A]): UserArg[A] = new NamedUserArgWithDefault[A](name, default)

  def arg[A](implicit le: ObjExt[A]): UserArg[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = le.`type`.fields.map({case (k, v) => UserInputArgument(k, v)})
      override def extract(ctx: FnContext) = le(ctx.params)
    }
  }

}

object ArgsInstances extends ArgsInstances