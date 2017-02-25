package io.hydrosphere.mist.lib

import spray.json.pimpAny
import io.hydrosphere.mist.utils.json.AnyJsonFormatSupport

trait Publisher extends AnyJsonFormatSupport {
  
  private[mist] def publish(message: String): Unit
  def publish(message: Map[String, Any]): Unit = {
    val json = message.toJson.compactPrint
    publish(json)
  }
}
