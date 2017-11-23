package mist.api.jdsl

import mist.api._
import mist.api.data.JsLikeData

class JJobDef[T](val jobDef: JobDef[RetVal[T]])

abstract class JMistJob[T] extends JArgsDef with JJobDefinition {

  def defineJob: JJobDef[T]

  final def execute(ctx: JobContext): JobResult[JsLikeData] = {
    defineJob.jobDef.invoke(ctx) match {
      case JobSuccess(v) => JobSuccess(v.encoded())
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }
}

