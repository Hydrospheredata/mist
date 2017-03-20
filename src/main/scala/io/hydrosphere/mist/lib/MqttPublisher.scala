package io.hydrosphere.mist.lib

import akka.actor.PoisonPill
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncInterface

trait MqttPublisher extends Publisher{
  override def publish(message: String): Unit = {
    if (MistConfig.Mqtt.isOn) {
      val publisher = AsyncInterface.publisher(AsyncInterface.Provider.Mqtt, null)
      publisher ! message
      publisher ! PoisonPill
    }
  }
}
