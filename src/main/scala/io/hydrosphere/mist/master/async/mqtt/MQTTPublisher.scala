package io.hydrosphere.mist.master.async.mqtt

import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.master.async.AsyncPublisher

private[mist] object MQTTPublisher {
  
  def props(mqttActorWrapper: ActorRef): Props = Props(classOf[MQTTPublisher], mqttActorWrapper)
  
}

private[mist] class MQTTPublisher(mqttActorWrapper: ActorRef) extends AsyncPublisher {
  override def send(message: String): Unit = {
    mqttActorWrapper ! new MQTTActorWrapper.Publish(message.getBytes("utf-8"))
  }
}
