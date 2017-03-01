package io.hydrosphere.mist.master.async.mqtt


import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.utils.{Logger, MultiReceiveActor}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.async.AsyncInterface.Provider
import io.hydrosphere.mist.master.async.{AsyncInterface, AsyncSubscriber}

private[mist] object MqttSubscriber {
  
  def props(publisherActor: ActorRef, mqttActorWrapper: ActorRef): Props = Props(classOf[MqttSubscriber], publisherActor, mqttActorWrapper)
  
}

private[mist] class MqttSubscriber(override val publisherActor: ActorRef, mqttActorWrapper: ActorRef) extends AsyncSubscriber with MultiReceiveActor with JobConfigurationJsonSerialization with Logger {
  
  override val provider: Provider = AsyncInterface.Provider.Mqtt

  override def preStart(): Unit = {
    mqttActorWrapper ! MqttActorWrapper.Subscribe(self)
    logger.debug("MqttSubscriber: starting")
  }
  
  

  receiver {
    case msg: MqttActorWrapper.Message =>
      val stringMessage = new String(msg.payload, "utf-8")
      logger.info("Receiving Data from MQTT, Topic : %s, Message : %s".format(MistConfig.Mqtt.subscribeTopic, stringMessage))
      processIncomingMessage(stringMessage)
  }

  override def postStop(): Unit = {
    super.postStop()
    
    logger.debug("MqttSubscriber: stopping")
  }
}
