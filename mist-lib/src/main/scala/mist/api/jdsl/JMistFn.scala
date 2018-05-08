package mist.api.jdsl

import mist.api._
import mist.api.data.JsData

import scala.util._

class JHandle(val underlying: LowHandle[RetVal])

abstract class JMistFn extends JArgsDef with JJobDefinition with FnEntryPoint {

  def handle: JHandle

  final def execute(ctx: FnContext): Try[JsData] = handle.underlying.invoke(ctx).map(_.encoded())
}

