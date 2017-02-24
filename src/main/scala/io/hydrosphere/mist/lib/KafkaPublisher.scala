package io.hydrosphere.mist.lib

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncInterface

trait KafkaPublisher extends Publisher {
  override def publish(message: String): Unit = {
    if (MistConfig.Mqtt.isOn) {
      AsyncInterface.publisher(AsyncInterface.Provider.Kafka, null) ! message
    }
  }
}
