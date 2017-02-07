package io.hydrosphere.mist.master.async.mqtt


import akka.actor.{ActorRef, Props}
import io.hydrosphere.mist.master.async.AsyncServiceActor
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.MistConfig

case object MQTTSubscribe

private[mist] class MQTTServiceActor extends AsyncServiceActor with JobConfigurationJsonSerialization with Logger {

  val pubsub: ActorRef = context.actorOf(Props(classOf[MQTTPubSub], s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}"))
  
  def receive: Receive = {

    case MQTTSubscribe => pubsub ! MQTTPubSub.Subscribe(self)

    case _ @ MQTTPubSub.SubscribeAck(MQTTPubSub.Subscribe(`self`)) => context become ready
  }

  def ready: Receive = {

    case msg: MQTTPubSub.Message =>
      val stringMessage = new String(msg.payload, "utf-8")
      logger.info("Receiving Data from MQTT, Topic : %s, Message : %s".format(MistConfig.MQTT.subscribeTopic, stringMessage))
      processIncomingMessage(stringMessage)
  }

  override def send(message: String): Unit = {
    pubsub ! new MQTTPubSub.Publish(message.getBytes("utf-8"))
  }
}
