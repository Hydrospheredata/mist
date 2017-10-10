package mist.api

import mist.api.data.MData

case class InputDescription(msg: String)

trait JobInfo {

  def describe(): InputDescription

}

abstract class MistJob[A](implicit enc: Encoder[A])
  extends JobInfo
  with JobDefInstances
  with Contexts {

  def defineJob: JobDef[A]

  def execute(ctx: JobContext): JobResult[MData] = {
    defineJob.invoke(ctx) match {
      case JobSuccess(data) => JobSuccess(enc(data))
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[MData]]
    }
  }

  override final def describe(): InputDescription = InputDescription("descr")

  def main(args: Array[String]) = {
    println("HELLO!")
  }
}

