package io.hydrosphere.mist.apiv2

case class InputDescription(msg: String)

trait JobInfo {

  def describe(): InputDescription

}

trait MistJob[A] extends JobInfo
  with JobDefInstances {

  def defineJob: JobDef[A]

  override final def describe(): InputDescription = InputDescription("descr")

  def main(args: Array[String]) = {
    println("HELLO!")
  }
}

