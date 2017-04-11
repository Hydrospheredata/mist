package io.hydrosphere.mist.master.interfaces.async

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import io.hydrosphere.mist.master.interfaces.async.kafka.{KafkaActorWrapper, KafkaPublisher, KafkaSubscriber}
import io.hydrosphere.mist.master.interfaces.async.mqtt.{MqttActorWrapper, MqttPublisher, MqttSubscriber}

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
  
  var system: ActorSystem = _
  
  def init(system: ActorSystem): Unit = {
    this.system = system
  }
  
  def subscriber(provider: Provider, inContext: Option[ActorContext] = None): ActorRef = provider match {
    case Provider.Mqtt =>
      inContext match {
        case Some(context) => context.actorOf(MqttSubscriber.props(publisher(provider, inContext), actorWrapper(provider, inContext)))
        case None => system.actorOf(MqttSubscriber.props(publisher(provider, inContext), actorWrapper(provider, inContext)))
      }
    case Provider.Kafka =>
      inContext match {
        case Some(context) => context.actorOf(KafkaSubscriber.props(publisher(provider, inContext), actorWrapper(provider, inContext)))
        case None => system.actorOf(KafkaSubscriber.props(publisher(provider, inContext), actorWrapper(provider, inContext))) 
      }
  }

  def publisher(provider: Provider, inContext: Option[ActorContext] = None): ActorRef = provider match {
    case Provider.Mqtt =>
        inContext match {
          case Some(context) => context.actorOf(MqttPublisher.props(actorWrapper(provider, inContext)))
          case None => system.actorOf(MqttPublisher.props(actorWrapper(provider, inContext))) 
        }
    case Provider.Kafka =>
        inContext match {
          case Some(context) => context.actorOf(KafkaPublisher.props())
          case None => system.actorOf(KafkaPublisher.props())
        }
  }
  
  private def actorWrapper(provider: Provider, inContext: Option[ActorContext] = None): ActorRef = provider match {
    case Provider.Mqtt =>
      inContext match {
        case Some(context) => context.actorOf(MqttActorWrapper.props())
        case None => system.actorOf(MqttActorWrapper.props())
      }
    case Provider.Kafka =>
      inContext match {
        case Some(context) => context.actorOf(KafkaActorWrapper.props())
        case None => system.actorOf(KafkaActorWrapper.props())
      }
    case x: Provider => throw new IllegalArgumentException(s"No wrapper for ${x.toString}")
  }
}
