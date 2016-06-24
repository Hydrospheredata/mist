package io.hydrosphere.mist.master

import akka.actor._
import akka.pattern.ask
import io.hydrosphere.mist.{Constants, MistConfig}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import io.hydrosphere.mist.jobs.{JobConfiguration, JobResult}
import org.eclipse.paho.client.mqttv3._

import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer

private[mist] object MqttPubSub {

  private[mist] class Publish(payload: Array[Byte]) {
    def message() = {
      val msg = new MqttMessage(payload)
      msg
    }
  }

  private[mist] case class Subscribe(ref: ActorRef)

  private[mist] case class SubscribeAck(subscribe: Subscribe)

  private[mist] class Message(val payload: Array[Byte])

  private[mist] case object Connect
  private[mist] case object Connected
  private[mist] case object Disconnected

  private[mist] class Subscribers extends Actor {
    private var subscribers = Set.empty[ActorRef]

    def receive = {
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

  private[mist] class PubSubMqttCallback(owner: ActorRef) extends MqttCallback {

    def connectionLost(cause: Throwable): Unit = {
      println("connection lost")
      owner ! Disconnected
    }

    def deliveryComplete(token: IMqttDeliveryToken): Unit = {

    }

    def messageArrived(topic: String, message: MqttMessage): Unit = {
      owner ! new Message(message.getPayload)
    }
  }

  private[mist] class ConnectionListener(owner: ActorRef) extends IMqttActionListener {

    def onSuccess(asyncActionToken: IMqttToken): Unit = {
      owner ! Connected
    }

    def onFailure(asyncActionToken: IMqttToken, e: Throwable): Unit = {
      owner ! Disconnected
    }
  }

  private[mist] object SubscribeListener extends IMqttActionListener {
    def onSuccess(asyncActionToken: IMqttToken): Unit = {
      println("subscribed to " + asyncActionToken.getTopics.mkString("[", ",", "]"))
    }

    def onFailure(asyncActionToken: IMqttToken, e: Throwable): Unit = {
      println("subscribe failed to " + asyncActionToken.getTopics.mkString("[", ",", "]"))
    }
  }
}

import MqttPubSub._

private[mist] class MqttPubSub(connectionUrl: String) extends Actor{

  lazy val connectionOptions = {
    val opt = new MqttConnectOptions
    opt.setCleanSession(true)
    opt
  }

  private[this] val client = {
    val c = new MqttAsyncClient(connectionUrl, MqttAsyncClient.generateClientId(), null)
    c.setCallback(new PubSubMqttCallback(self))
    c
  }

  private[this] val connectionListener = new ConnectionListener(self)

  private[this] val msgBuffer = ListBuffer.empty[(Deadline, Any)]

  def receive = {
    case Connect => {
      println(s"connecting to ${connectionUrl}..")
      try {
        client.connect(connectionOptions, null, connectionListener)
      } catch {
        case e: Exception => println(s"can't connect to $connectionUrl")
      }
    }
    case Connected => {
      for ((deadline, x) <- msgBuffer if deadline.hasTimeLeft()) self ! x
      msgBuffer.clear()
      context become ready
    }

    case x @ (_: Publish | _: Subscribe) => {
      if (msgBuffer.length > 1000) {
        msgBuffer.remove(0, 500)
      }
      msgBuffer += Tuple2(Deadline.now + 1.day, x)
    }
  }

  def ready : Receive  = {

    case p: Publish => {
      try {
        client.publish(MistConfig.MQTT.publishTopic, p.message())
      } catch {
        case e: Exception => println(s"can't publish to ${MistConfig.MQTT.publishTopic}")
      }
    }

    case msg@Subscribe(ref) => {

      context.child(Constants.Actors.mqttServiceName) match {
        case Some(t) => t ! msg
        case None =>
          val t = context.actorOf(Props[Subscribers], name = Constants.Actors.mqttServiceName)
          t ! msg
          context watch t
          try {
            client.subscribe(MistConfig.MQTT.publishTopic, 0, null, SubscribeListener)
          } catch {
            case e: Exception => println(e); println(s"can't subscribe to ${MistConfig.MQTT.publishTopic}")
          }
      }
    }

    case msg: Message => context.child(Constants.Actors.mqttServiceName) foreach (_ ! msg)

    case Terminated(topicRef) => {
      try {
        client.unsubscribe(Constants.Actors.mqttServiceName)
      } catch {
        case e: Exception => println(s"can't unsubscribe from ${topicRef.path.name}")
      }
    }

    case Disconnected => {
      context become receive
      self ! Connect
    }
  }

  self ! Connect
}

private[mist] case object MqttSubscribe


private[mist] trait MqttPubSubActor { this: Actor =>
  val pubsub = context.actorOf(Props(classOf[MqttPubSub], s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}"))
}

private[mist] class MQTTServiceActor extends Actor with MqttPubSubActor with JsonFormatSupport{

  def receive = {

    case MqttSubscribe => pubsub ! Subscribe(self)

    case msg @ SubscribeAck(Subscribe(`self`)) => context become ready
  }

  def ready: Receive = {

    case msg: Message => {

      val stringMessage = new String(msg.payload, "utf-8")

      println("Receiving Data, Topic : %s, Message : %s".format(MistConfig.MQTT.publishTopic, stringMessage))
      try {
        val json = stringMessage.parseJson
        // map request into JobConfiguration
        val jobCreatingRequest = json.convertTo[JobConfiguration]

        val workerManagerActor = context.system.actorSelection(s"akka://mist/user/${Constants.Actors.workerManagerName}")
        // Run job asynchronously
        val future = workerManagerActor.ask(jobCreatingRequest)(timeout = MistConfig.Contexts.timeout(jobCreatingRequest.name))

        future
          .recover {
            case error: Throwable => Right(error.toString)
          }
          .onSuccess {
            case result: Either[Map[String, Any], String] =>
              val jobResult: JobResult = result match {
                case Left(jobResult: Map[String, Any]) =>
                  JobResult(success = true, payload = jobResult, request = jobCreatingRequest, errors = List.empty)
                case Right(error: String) =>
                  JobResult(success = false, payload = Map.empty[String, Any], request = jobCreatingRequest, errors = List(error))
              }

              val jsonString = Json(DefaultFormats).write(jobResult)
              pubsub ! new Publish(jsonString.getBytes("utf-8"))
          }
      }
      catch {

        case _: spray.json.JsonParser.ParsingException => println("BadJson")

        case _: DeserializationException => println("DeserializationException Bad type in Json")
      }

    }
  }
}