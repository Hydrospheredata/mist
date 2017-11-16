package mist.api

import io.hydrosphere.mist.api.SetupConfiguration

sealed trait JobContext{
  val params: Map[String, Any]
}

case class FullJobContext(
  setupConf: SetupConfiguration,
  params: Map[String, Any]
) extends JobContext

object JobContext {
  def apply(userParams: Map[String, Any]): JobContext = new JobContext {
    override val params: Map[String, Any] = userParams
  }

  def apply(setupConf: SetupConfiguration, params: Map[String, Any]) =
    FullJobContext(setupConf, params)
}
