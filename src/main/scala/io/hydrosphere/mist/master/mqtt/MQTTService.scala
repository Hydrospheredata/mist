package io.hydrosphere.mist.master.mqtt

import akka.actor.{Actor, Props}
import akka.pattern.ask
import io.hydrosphere.mist.jobs.{JobConfiguration, JobResult}
import io.hydrosphere.mist.master.JsonFormatSupport
import io.hydrosphere.mist.{Constants, MistConfig}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json.{DeserializationException, pimpString}

import scala.concurrent.ExecutionContext.Implicits.global
import io.hydrosphere.mist.Logger


private[mist] case object MqttSubscribe

private[mist] trait MqttPubSubActor { this: Actor =>
  val pubsub = context.actorOf(Props(classOf[MqttPubSub], s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}"))
}

private[mist] class MQTTServiceActor extends Actor with MqttPubSubActor with JsonFormatSupport with Logger{

  def receive: Receive = {

    case MqttSubscribe => pubsub ! MqttPubSub.Subscribe(self)

    case msg @ MqttPubSub.SubscribeAck(MqttPubSub.Subscribe(`self`)) => context become ready
  }

  def ready: Receive = {

    case msg: MqttPubSub.Message =>

      val stringMessage = new String(msg.payload, "utf-8")

      logger.info("Receiving Data, Topic : %s, Message : %s".format(MistConfig.MQTT.publishTopic, stringMessage))
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
              pubsub ! new MqttPubSub.Publish(jsonString.getBytes("utf-8"))
          }
      }
      catch {

        case _: spray.json.JsonParser.ParsingException => logger.error("BadJson")

        case _: DeserializationException => logger.error("DeserializationException Bad type in Json")
      }
  }
}
