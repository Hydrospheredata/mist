package io.hydrosphere.mist.actors

import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.pattern.ask
import io.hydrosphere.mist.{Constants, MistConfig}
import io.hydrosphere.mist.actors.tools.{JSONSchemas, JSONValidator}

import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

import io.hydrosphere.mist.jobs.{JobResult, JobConfiguration}
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence


//TODO: must be an akka actor
private[mist] object MQTTService {

  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = scala.util.Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }
  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }

  def publish(message: String) = {
    var client: MqttClient = null

    val persistence = new MemoryPersistence

    try {
      // mqtt client with specific url and client id

      client = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", randomAlphaNumericString(16), persistence)

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

    val client = new MqttClient(s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}", randomAlphaNumericString(16), persistence)

    client.connect

    client.subscribe(MistConfig.MQTT.subscribeTopic)

    val callback = new MqttCallback {
      override def messageArrived(topic: String, message: MqttMessage): Unit = {
        println("Receiving Data, Topic : %s, Message : %s".format(topic, message))

        val stringMessage = message.toString

        // we need to check if message is a request
        val isMessageValidJar = JSONValidator.validate(stringMessage, JSONSchemas.jobRequest)
        val isMessageValidPy = JSONValidator.validate(stringMessage, JSONSchemas.jobRequestPy)
        // if it a request
        if (isMessageValidJar || isMessageValidPy) {
          implicit val formats = Serialization.formats(NoTypeHints)
          val json = parse(stringMessage)
          // map request into JobConfiguration
          val jobCreatingRequest = json.extract[JobConfiguration]

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

                val jsonString = write(jobResult)
                MQTTService.publish(jsonString)
            }
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