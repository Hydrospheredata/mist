package io.hydrosphere.mist.master.async.mqtt


import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.utils.{Logger, MultiReceiveActor}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.async.{AsyncInterface, AsyncSubscriber}

private[mist] object MQTTSubscriber {
  
  def props(publisherActor: ActorRef, mqttActorWrapper: ActorRef): Props = Props(classOf[MQTTSubscriber], publisherActor, mqttActorWrapper)
  
}

private[mist] class MQTTSubscriber(override val publisherActor: ActorRef, mqttActorWrapper: ActorRef) extends AsyncSubscriber with MultiReceiveActor with JobConfigurationJsonSerialization with Logger {
  
  override val provider: Provider = AsyncInterface.Provider.Mqtt

  override def preStart(): Unit = {
    mqttActorWrapper ! MQTTActorWrapper.Subscribe(self)
  }

  receiver {
    case msg: MQTTActorWrapper.Message =>
      val stringMessage = new String(msg.payload, "utf-8")
      logger.info("Receiving Data from MQTT, Topic : %s, Message : %s".format(MistConfig.MQTT.subscribeTopic, stringMessage))
      processIncomingMessage(stringMessage)
  }

}
