package mist.api.jdsl

import mist.api._
import mist.api.data.JsData

import scala.util._

abstract class JMistFn extends FnEntryPoint {

  def handle: Handle

  final def execute(ctx: FnContext): Try[JsData] = handle.underlying.invoke(ctx).map(_.encoded())
}

