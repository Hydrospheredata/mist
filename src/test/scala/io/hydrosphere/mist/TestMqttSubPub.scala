package io.hydrosphere.mist

import akka.actor.ActorSystem
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttMessage, MqttCallback, IMqttDeliveryToken}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

object MqttSuccessObj{
  var success: Boolean = false
  var ready_pub: Boolean = false
  var ready_sub: Boolean = false
}

object MQTTTest{
  def publish(message: String): Unit = {
    val persistence = new MemoryPersistence
    try {
      val client = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", MqttClient.generateClientId, persistence)
      client.connect()
      val msgTopic = client.getTopic(MistConfig.MQTT.publishTopic)
      val mqMessage = new MqttMessage(message.getBytes("utf-8"))
      msgTopic.publish(mqMessage)
      println(s"Publishing Data Test, Topic : ${msgTopic.getName}, Message : $mqMessage")
    }
  }

  def subscribe(actorSystem: ActorSystem): Unit = {
    val persistence = new MemoryPersistence
    val mqttClient = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", MqttClient.generateClientId, persistence)
    mqttClient.connect()
    mqttClient.subscribe(MistConfig.MQTT.subscribeTopic)
    val callback = new MqttCallback {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        println("Receiving Data Test, Topic : %s, Message : %s".format(topic, message))
        val stringMessage = message.toString
        stringMessage.split(':').drop(1).head.split(',').headOption.getOrElse("false") match {
          case "true" => MqttSuccessObj.success = true
          case "false" => MqttSuccessObj.success = false
          case _ =>
        }
      }

      override def connectionLost(cause: Throwable): Unit = {
        println(cause)
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
      }
    }
    mqttClient.setCallback(callback)
  }
}

