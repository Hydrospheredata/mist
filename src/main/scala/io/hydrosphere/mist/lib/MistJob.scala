package io.hydrosphere.mist.lib

trait MistJob extends ContextSupport {
  def doStuff(parameters: Map[String, Any]): Map[String, Any]
}
