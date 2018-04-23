package mist.api

import mist.api.data.JsData
import mist.api.encoding.{JsExtractor, RootExtractor}

trait ArgsInstances {

  def const[A](value: A): ArgDef[A] = SystemArg(Seq.empty, Extracted(value))
  def missing[A](message: String): ArgDef[A] = SystemArg(Seq.empty, Failed.InvalidType(message, "no value"))

  def arg[A](name: String)(implicit ext: JsExtractor[A]): UserArg[A] = new NamedUserArg[A](name)
  def arg[A](name: String, default: A)(implicit ext: JsExtractor[A]): UserArg[A] = new NamedUserArgWithDefault[A](name, default)

  def arg[A](implicit le: RootExtractor[A]): UserArg[A] = {
    new UserArg[A] {
      override def describe(): Seq[ArgInfo] = le.`type`.fields.map({case (k, v) => UserInputArgument(k, v)})
      override def extract(ctx: FnContext) = le(ctx.params)
    }
  }

  def allArgs: ArgDef[Map[String, Any]] = SystemArg(Seq.empty, ctx => Extracted(JsData.untyped(ctx.params)))

}

object ArgsInstances extends ArgsInstances