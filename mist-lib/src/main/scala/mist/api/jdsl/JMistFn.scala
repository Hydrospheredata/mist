package mist.api.jdsl

import mist.api._
import mist.api.data.JsLikeData

import scala.util._

class JHandle(val underlying: LowHandle[RetVal])

abstract class JMistFn extends JArgsDef with JJobDefinition {

  def handle: JHandle

  final def execute(ctx: FnContext): Try[JsLikeData] = handle.underlying.invoke(ctx).map(_.encoded())
}

