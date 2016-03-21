package com.provectus.mist.test

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{ActorRef, Actor}
import com.provectus.mist.MistConfig
import net.sigusr.mqtt.api._

object MqttSuccessObj{
  var success: Boolean = false
}

class MQTTTestSub extends Actor {
  val mqttClient = context.actorOf(Manager.props(new InetSocketAddress(InetAddress.getByName(MistConfig.MQTT.host), MistConfig.MQTT.port)))
  mqttClient ! Connect(TestConfig.mqtt_test_sub_name)

  override def receive: Receive = {

    case Connected =>
      println("Sub connected to mqtt")
      sender() ! Subscribe(Vector((MistConfig.MQTT.subscribeTopic, AtMostOnce)), 1)
      context become ready(sender())

    case ConnectionFailure(reason) => println(s"Sub connection to mqtt failed [$reason]")

  }

  def ready(mqttManager: ActorRef): Receive = {

    case Subscribed(vQoS, MessageId(1)) =>
      println("Test sub successfully subscribed")

    case Message(topic, payload) =>

      val stringMessage = new String(payload.to[Array], "UTF-8")
      println(s"[$topic] $stringMessage")
      stringMessage match {
        case TestConfig.mqtt_try_response => {
          MqttSuccessObj.success = true
        }
        case _ => MqttSuccessObj.success = false
      }
  }
}


class MQTTTestPub extends Actor {

  val mqttClient = context.actorOf(Manager.props(new InetSocketAddress(InetAddress.getByName(MistConfig.MQTT.host), MistConfig.MQTT.port)))
  mqttClient ! Connect(TestConfig.mqtt_test_pub_name)

  override def receive: Receive = {

    case Connected =>
      println("Publisher connected to mqtt")
      context become ready(sender())

    case ConnectionFailure(reason) => println(s"Pub connection to mqtt failed [$reason]")
  }

  def ready(mqttManager: ActorRef): Receive = {

    case msg: String =>
      println(msg)
      mqttClient ! Publish(MistConfig.MQTT.publishTopic, msg.getBytes("UTF-8").to[Vector])
  }
}
