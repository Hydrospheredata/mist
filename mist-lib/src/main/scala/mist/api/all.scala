package mist.api

import mist.api.args.{ArgsInstances, WithArgsScala}
import mist.api.encoding.DefaultEncoders

object all extends ArgsInstances
  with MistExtrasDef
  with WithArgsScala
  with Contexts
  with DefaultEncoders {

  type ArgDef[A] = args.ArgDef[A]
  val ArgDef = args.ArgDef

  type MistFn = mist.api.MistFn
  type MistExtras = mist.api.MistExtras
  val MistExtras = mist.api.MistExtras

  type Handle = mist.api.Handle
}
