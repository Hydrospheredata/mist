package mist.api

import mist.api.data.MData

case class InputDescription(msg: String)

abstract class MistJob[A](implicit enc: Encoder[A])
  extends JobDefInstances
  with Contexts {

  def defineJob: JobDef[A]

  def execute(ctx: JobContext): JobResult[MData] = {
    defineJob.invoke(ctx) match {
      case JobSuccess(data) => JobSuccess(enc(data))
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[MData]]
    }
  }

  def main(args: Array[String]) = {
    println("HELLO!")
  }
}

