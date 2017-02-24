package io.hydrosphere.mist.master.async

import akka.actor.{ActorRef, ActorSystem}
import io.hydrosphere.mist.master.async.kafka.{KafkaPublisher, KafkaSubscriber}
import io.hydrosphere.mist.master.async.mqtt.{MqttActorWrapper, MqttPublisher, MqttSubscriber}

private[mist] object AsyncInterface {

  sealed trait Provider
  object Provider {
    
    def apply(string: String): Provider = string match {
      case "mqtt" => Mqtt
      case "kafka" => Kafka
    }
    
    case object Mqtt extends Provider {
      override def toString: String = "mqtt"
    }
    case object Kafka extends Provider {
      override def toString: String = "kafka"
    }
  }
  
  private var mqttActorWrapper: ActorRef = _
  private var mqttSubscriber: ActorRef = _
  private var mqttPublisher: ActorRef = _
  
  private var kafkaSubscriber: ActorRef = _
  private var kafkaPublisher: ActorRef = _
  
  def subscriber(provider: Provider, system: ActorSystem): ActorRef = provider match {
    case Provider.Mqtt =>
      if (mqttSubscriber == null) {
        mqttActorWrapper = system.actorOf(MqttActorWrapper.props())
        mqttSubscriber = system.actorOf(MqttSubscriber.props(publisher(provider, system), actorWrapper(provider, system)))
      }
      mqttSubscriber
    case Provider.Kafka =>
      if (kafkaSubscriber == null) {
        kafkaSubscriber = system.actorOf(KafkaSubscriber.props(publisher(provider, system)))
      }
      kafkaSubscriber
  }

  def publisher(provider: Provider, system: ActorSystem): ActorRef = provider match {
    case Provider.Mqtt =>
      if (mqttPublisher == null) {
        if (system == null) {
          throw new IllegalArgumentException("ActorSystem cannot be null before initializing the actor")
        }
        mqttPublisher = system.actorOf(MqttPublisher.props(actorWrapper(provider, system)))
      }
      mqttPublisher
    case Provider.Kafka =>
      if (kafkaPublisher == null) {
        if (system == null) {
          throw new IllegalArgumentException("ActorSystem cannot be null before initializing the actor")
        }
        kafkaPublisher = system.actorOf(KafkaPublisher.props())
      }
      kafkaPublisher
  }
  
  private def actorWrapper(provider: Provider, system: ActorSystem): ActorRef = provider match {
    case Provider.Mqtt =>
      if (mqttActorWrapper == null) {
        mqttActorWrapper = system.actorOf(MqttActorWrapper.props())
      }
      mqttActorWrapper
    case x: Provider => throw new IllegalArgumentException(s"No wrapper for ${x.toString}")
  }
}
