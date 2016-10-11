package io.hydrosphere.mist.lib

import io.hydrosphere.mist.MistConfig
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

trait Publisher {

  def publish(message: String): Unit = {
    val persistence = new MemoryPersistence
    try {
      val client = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", MqttClient.generateClientId, persistence)
      client.connect()
      val msgTopic = client.getTopic(MistConfig.MQTT.publishTopic)
      val mqMessage = new MqttMessage(message.getBytes("utf-8"))
      msgTopic.publish(mqMessage)
      client.disconnect()
    }
  }
}
