package io.hydrosphere.mist.master.async.mqtt

import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.master.async.AsyncPublisher

private[mist] object MqttPublisher {
  
  def props(mqttActorWrapper: ActorRef): Props = Props(classOf[MqttPublisher], mqttActorWrapper)
  
}

private[mist] class MqttPublisher(mqttActorWrapper: ActorRef) extends AsyncPublisher {
  override def send(message: String): Unit = {
    mqttActorWrapper ! new MqttActorWrapper.Publish(message.getBytes("utf-8"))
  }
}
