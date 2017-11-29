package mist.api.jdsl

import mist.api._
import mist.api.data.JsLikeData

class JFnDef[T](val jobDef: FnDef[RetVal[T]])

abstract class JMistFn[T] extends JArgsDef with JJobDefinition {

  def handler: JFnDef[T]

  final def execute(ctx: FnContext): JobResult[JsLikeData] = {
    handler.jobDef.invoke(ctx) match {
      case JobSuccess(v) => JobSuccess(v.encoded())
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }
}

