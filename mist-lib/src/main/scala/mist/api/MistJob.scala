package mist.api

case class InputDescription(msg: String)

trait JobInfo {

  def describe(): InputDescription

}

trait MistJob[A] extends JobInfo with JobDefInstances with DefaultEncoders {

  def defineJob: JobDef[A]

  override final def describe(): InputDescription = InputDescription("descr")

  def main(args: Array[String]) = {
    println("HELLO!")
  }
}

