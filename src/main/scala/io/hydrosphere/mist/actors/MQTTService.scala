package io.hydrosphere.mist.actors

import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.pattern.ask
import io.hydrosphere.mist.{Constants, MistConfig}
import io.hydrosphere.mist.actors.tools.{JSONSchemas, JSONValidator}

import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json._

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

import io.hydrosphere.mist.jobs.{JobResult, JobConfiguration}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence


//TODO: must be an akka actor
private[mist] object MQTTService extends DefaultJsonProtocol {

  // TODO: remove copy/paste from HTTP actor
  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case number: Int => JsNumber(number)
      case string: String => JsString(string)
      case sequence: Seq[_] => seqFormat[Any].write(sequence)
      case map: Map[String, _] => mapFormat[String, Any] write map
      case boolean: Boolean if boolean => JsTrue
      case boolean: Boolean if !boolean => JsFalse
      case unknown => serializationError("Do not understand object of type " + unknown.getClass.getName)
    }
    def read(value: JsValue) = value match {
      case JsNumber(number) => number.toBigInt()
      case JsString(string) => string
      case array: JsArray => listFormat[Any].read(value)
      case jsObject: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case unknown => deserializationError("Do not understand how to deserialize " + unknown)
    }
  }

  implicit val jobCreatingRequestFormat = jsonFormat6(JobConfiguration)

  def publish(message: String) = {
    var client: MqttClient = null

    val persistence = new MemoryPersistence

    try {
      // mqtt client with specific url and client id
      client = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", MqttClient.generateClientId, persistence)

      client.connect

      val msgTopic = client.getTopic(MistConfig.MQTT.publishTopic)
      val mqMessage = new MqttMessage(message.getBytes("utf-8"))

      msgTopic.publish(mqMessage)
      println(s"Publishing Data, Topic : ${msgTopic.getName}, Message : $mqMessage")
    }

    catch {
      case e: MqttException => println("Exception Caught: " + e)
    }

    finally {
      client.disconnect
    }
  }

  def subscribe(actorSystem: ActorSystem) = {
    lazy val jobRequestActor: ActorRef = actorSystem.actorOf(Props[JobRunner], name = Constants.Actors.asyncJobRunnerName)

    val persistence = new MemoryPersistence

    val client = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", MqttClient.generateClientId, persistence)

    client.connect

    client.subscribe(MistConfig.MQTT.subscribeTopic)

    val callback = new MqttCallback {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        println("Receiving Data, Topic : %s, Message : %s".format(topic, message))

        val stringMessage = message.toString

          val json = stringMessage.parseJson
          // map request into JobConfiguration
          val jobCreatingRequest = {
            try {
              json.convertTo[JobConfiguration]
            } catch {
              case _: DeserializationException => return // pass invalid json
            }
          }

          // Run job asynchronously
          val future = jobRequestActor.ask(jobCreatingRequest)(timeout = MistConfig.Contexts.timeout(jobCreatingRequest.name))

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
                MQTTService.publish(jsonString)
            }
      }

      override def connectionLost(cause: Throwable): Unit = {
        println(cause)
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {

      }
    }

    client.setCallback(callback)
  }
}
