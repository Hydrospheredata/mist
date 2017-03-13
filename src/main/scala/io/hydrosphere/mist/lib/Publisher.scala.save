package io.hydrosphere.mist.lib

import org.json4s.DefaultFormats
import org.json4s.native.Json

trait Publisher {
  private[mist] def publish(message: String): Unit
  def publish(message: Map[String, Any]): Unit = {
    val json = Json(DefaultFormats).write(message)
    publish(json)
  }
}
