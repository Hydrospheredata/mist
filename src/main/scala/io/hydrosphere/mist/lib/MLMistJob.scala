package io.hydrosphere.mist.lib

trait MLMistJob extends ContextSupport {

  def train(params: Map[String, Any]): Map[String, Any]

  def serve(params: Map[String, Any]): Map[String, Any]
  
}
