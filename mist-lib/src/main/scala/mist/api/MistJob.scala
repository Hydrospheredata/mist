package mist.api

import mist.api.data.MData

abstract class MistJob[A](implicit enc: Encoder[A])
  extends JobDefInstances
  with Contexts {

  def defineJob: JobDef[A]

  final def execute(ctx: JobContext): JobResult[MData] = {
    defineJob.invoke(ctx) match {
      case JobSuccess(data) => JobSuccess(enc(data))
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[MData]]
    }
  }

  def main(args: Array[String]) = {
    println("HELLO!")
  }
}

