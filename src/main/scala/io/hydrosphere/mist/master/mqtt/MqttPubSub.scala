package io.hydrosphere.mist.master.mqtt

import akka.actor.{Actor, ActorRef, Props, Terminated}
import io.hydrosphere.mist.{Constants, Logger, MistConfig}
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import org.eclipse.paho.client.mqttv3._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Deadline, DurationInt}


private[mist] class MqttPubSub(connectionUrl: String) extends Actor with Logger{

  lazy val connectionOptions = {
    val opt = new MqttConnectOptions
    opt.setCleanSession(true)
    opt
  }

  private[this] val client = {
    val persistence = new MemoryPersistence
    val client = new MqttAsyncClient(connectionUrl, MqttAsyncClient.generateClientId(), persistence)
    client.setCallback(new MqttPubSub.Callback(self))
    client
  }

  private[this] val connectionListener = new MqttPubSub.ConnectionListener(self)

  private[this] val msgBuffer = ListBuffer.empty[(Deadline, Any)]

  def receive: Receive = {
    case MqttPubSub.Connect =>
      logger.info(s"connecting to $connectionUrl..")
      try {
        client.connect(connectionOptions, None, connectionListener)
      } catch {
        case e: Exception => logger.error(s"can't connect to $connectionUrl")
      }

    case MqttPubSub.Connected =>
      for ((deadline, x) <- msgBuffer if deadline.hasTimeLeft()) {
        self ! x
      }
      msgBuffer.clear()
      context become ready

    case x @ (_: MqttPubSub.Publish | _: MqttPubSub.Subscribe) =>
      if (msgBuffer.length > 1000) {
        msgBuffer.remove(0, msgBuffer.length/2)
      }
      msgBuffer += Tuple2(Deadline.now + 1.day, x)
  }

  def ready: Receive = {

    case p: MqttPubSub.Publish =>
      try {
        client.publish(MistConfig.MQTT.publishTopic, p.message())
      } catch {
        case e: Exception => logger.error(s"can't publish to ${MistConfig.MQTT.publishTopic}")
      }

    case msg@MqttPubSub.Subscribe(ref) =>

      context.child(Constants.Actors.mqttServiceName) match {
        case Some(t) => t ! msg
        case None =>
          val t = context.actorOf(Props[MqttPubSub.Subscribers], name = Constants.Actors.mqttServiceName)
          t ! msg
          context watch t
          try {
            client.subscribe(MistConfig.MQTT.subscribeTopic, 0, None, MqttPubSub.SubscribeListener)
          } catch {
            case e: Exception => logger.error(s"can't subscribe to ${MistConfig.MQTT.subscribeTopic}", e)
          }
      }

    case msg: MqttPubSub.Message => context.child(Constants.Actors.mqttServiceName) foreach (_ ! msg)

    case Terminated(topicRef) =>
      try {
        client.unsubscribe(Constants.Actors.mqttServiceName)
      } catch {
        case e: Exception => logger.error(s"can't unsubscribe from ${topicRef.path.name}", e)
      }

    case MqttPubSub.Disconnected =>
      context become receive
      self ! MqttPubSub.Connect
  }

  self ! MqttPubSub.Connect
}

private[mist] object MqttPubSub {

  class Publish(payload: Array[Byte]) {
    def message(): MqttMessage = {
      new MqttMessage(payload)
    }
  }

  case class Subscribe(ref: ActorRef)

  case class SubscribeAck(subscribe: Subscribe)

  class Message(val payload: Array[Byte])

  case object Connect
  case object Connected
  case object Disconnected

  class Subscribers extends Actor {
    private var subscribers = Set.empty[ActorRef]

    def receive: Receive = {
      case msg: Message =>
        subscribers foreach (_ ! msg)

      case msg @ Subscribe(ref) =>
        context watch ref
        subscribers += ref
        ref ! SubscribeAck(msg)

      case Terminated(ref) =>
        subscribers -= ref
        if (subscribers.isEmpty) context stop self
    }
  }

  class Callback(owner: ActorRef) extends MqttCallback with Logger{

    def connectionLost(cause: Throwable): Unit = {
      logger.info("connection lost")
      owner ! Disconnected
    }

    def deliveryComplete(token: IMqttDeliveryToken): Unit = {

    }

    def messageArrived(topic: String, message: MqttMessage): Unit = {
      owner ! new Message(message.getPayload)
    }
  }

  class ConnectionListener(owner: ActorRef) extends IMqttActionListener {

    def onSuccess(asyncActionToken: IMqttToken): Unit = {
      owner ! Connected
    }

    def onFailure(asyncActionToken: IMqttToken, e: Throwable): Unit = {
      owner ! Disconnected
    }
  }

  object SubscribeListener extends IMqttActionListener with Logger{
    def onSuccess(asyncActionToken: IMqttToken): Unit = {
      logger.info("subscribed to " + asyncActionToken.getTopics.mkString("[", ",", "]"))
    }

    def onFailure(asyncActionToken: IMqttToken, e: Throwable): Unit = {
      logger.info("subscribe failed to " + asyncActionToken.getTopics.mkString("[", ",", "]"))
    }
  }
}

