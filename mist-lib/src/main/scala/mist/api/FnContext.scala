package mist.api

import io.hydrosphere.mist.api.SetupConfiguration

sealed trait FnContext{
  val params: Map[String, Any]
}

case class FullFnContext(
  setupConf: SetupConfiguration,
  params: Map[String, Any]
) extends FnContext

object FnContext {
  def apply(userParams: Map[String, Any]): FnContext = new FnContext {
    override val params: Map[String, Any] = userParams
  }

  def apply(setupConf: SetupConfiguration, params: Map[String, Any]) =
    FullFnContext(setupConf, params)
}
