package io.hydrosphere.mist.lib

import io.hydrosphere.mist.MistConfig
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

trait MqttPublisher extends Publisher{
  override def publish(message: String): Unit = {
    if (MistConfig.Mqtt.isOn) {
      val persistence = new MemoryPersistence
      val client = new MqttClient(s"tcp://${MistConfig.Mqtt.host}:${MistConfig.Mqtt.port}", MqttClient.generateClientId, persistence)
      client.connect()
      val msgTopic = client.getTopic(MistConfig.Mqtt.publishTopic)
      val mqMessage = new MqttMessage(message.getBytes("utf-8"))
      msgTopic.publish(mqMessage)
      client.disconnect()
    }
  }
}
