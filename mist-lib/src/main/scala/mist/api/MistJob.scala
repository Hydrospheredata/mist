package mist.api

import mist.api.data.JsLikeData

abstract class MistJob[A](implicit enc: Encoder[A])
  extends JobDefInstances
  with Contexts
  with MistExtrasDef {

  def defineJob: JobDef[A]

  final def execute(ctx: JobContext): JobResult[JsLikeData] = {
    defineJob.invoke(ctx) match {
      case JobSuccess(data) => JobSuccess(enc(data))
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }

  def main(args: Array[String]) = {
    println("HELLO!")
  }
}

