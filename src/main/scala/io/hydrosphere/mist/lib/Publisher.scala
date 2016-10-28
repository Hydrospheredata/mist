package io.hydrosphere.mist.lib

trait Publisher {
  def publish(message: String): Unit
}

