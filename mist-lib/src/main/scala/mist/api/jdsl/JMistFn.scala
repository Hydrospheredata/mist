package mist.api.jdsl

import mist.api._
import mist.api.data.JsLikeData

class JHandle[T](val underlying: Handle[RetVal[T]])

abstract class JMistFn[T] extends JArgsDef with JJobDefinition {

  def handle: JHandle[T]

  final def execute(ctx: FnContext): JobResult[JsLikeData] = {
    handle.underlying.invoke(ctx) match {
      case JobSuccess(v) => JobSuccess(v.encoded())
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }
}

