package com.provectus.mist.test

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{ActorRef, Actor}
import com.provectus.mist.MistConfig
import net.sigusr.mqtt.api._

object MqttSuccessObj{
  var success: Boolean = false
  var ready_pub: Boolean = false
  var ready_sub: Boolean = false
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

    case Subscribed(vQoS, MessageId(1)) => {
      println("Test sub successfully subscribed")
      MqttSuccessObj.ready_sub = true
    }

    case Message(topic, payload) =>

      val stringMessage = new String(payload.to[Array], "UTF-8")
      println(s"[$topic] $stringMessage")
      stringMessage.split(':').drop(1).head.split(',').headOption.getOrElse("false") match {
        case "true" =>  MqttSuccessObj.success = true
        case "false" => MqttSuccessObj.success = false
        case _ =>
      }
  }
}

class MQTTTestPub extends Actor {

  val mqttClient = context.actorOf(Manager.props(new InetSocketAddress(InetAddress.getByName(MistConfig.MQTT.host), MistConfig.MQTT.port)))
  mqttClient ! Connect(TestConfig.mqtt_test_pub_name)

  override def receive: Receive = {

    case Connected =>
      println("Publisher connected to mqtt")
      MqttSuccessObj.ready_pub = true
      context become ready(sender())

    case ConnectionFailure(reason) => println(s"Pub connection to mqtt failed [$reason]")
  }

  def ready(mqttManager: ActorRef): Receive = {

    case msg: String => mqttClient ! Publish(MistConfig.MQTT.publishTopic, msg.getBytes("UTF-8").to[Vector])
  }
}
