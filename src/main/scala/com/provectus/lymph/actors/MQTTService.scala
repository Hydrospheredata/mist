package com.provectus.lymph.actors

import java.net.{InetAddress, InetSocketAddress}

import akka.actor.{Props, ActorRef, Actor}
import akka.pattern.ask
import com.provectus.lymph.LymphConfig
import com.provectus.lymph.actors.tools.{JSONSchemas, JSONValidator}

import org.json4s.NoTypeHints
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import net.sigusr.mqtt.api._

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

import com.provectus.lymph.jobs.{JobResult, JobConfiguration}

/** MQTT interface */
private[lymph] class MQTTService extends Actor {

  // Connect to MQTT with host/port from config
  context.actorOf(Manager.props(new InetSocketAddress(InetAddress.getByName(LymphConfig.MQTT.host), LymphConfig.MQTT.port))) ! Connect("MQTTService")

  override def receive: Receive = {
    // Connected to MQTT server
    case Connected =>
      println("Connected to mqtt")
      // Subscribing to MQTT topic
      sender() ! Subscribe(Vector((LymphConfig.MQTT.subscribeTopic, AtMostOnce)), 1)
      // We are ready to receive message from MQ server
      context become ready(sender())
    case ConnectionFailure(reason) => println(s"Connection to mqtt failed [$reason]")
  }

  // actor which is used for running jobs according to request
  lazy val jobRequestActor: ActorRef = context.actorOf(Props[JobRunner], name = "AsyncJobRunner")

  def ready(mqttManager: ActorRef): Receive = {

    // Subscribed to MQTT topic
    case Subscribed(vQoS, MessageId(1)) =>
      println("Successfully subscribed to topic foo")

    // Received a message
    case Message(topic, payload) =>
      val stringMessage = new String(payload.to[Array], "UTF-8")
      println(s"[$topic] $stringMessage")

      // we need to check if message is a request
      val isMessageValid = JSONValidator.validate(stringMessage, JSONSchemas.jobRequest)

      // if it a request
      if (isMessageValid) {
        implicit val formats = Serialization.formats(NoTypeHints)
        val json = parse(stringMessage)
        // map request into JobConfiguration
        val jobCreatingRequest = json.extract[JobConfiguration]

        // TODO: catch timeout exception
        // Run job asynchronously
        val future = jobRequestActor.ask(jobCreatingRequest)(timeout = LymphConfig.Contexts.timeout(jobCreatingRequest.name))

        future.andThen {
          // Future is ok and result is Map, so everything ok, we can send a response
          case Success(result: Map[String, Any]) =>
            val jobResult = JobResult(success = true, payload = result, request = jobCreatingRequest, errors = List.empty)
            val jsonString = write(jobResult)
            mqttManager ! Publish(LymphConfig.MQTT.publishTopic, jsonString.getBytes("UTF-8").to[Vector])
            println(s"ok, result: ${write(result)}")
          // Future is ok but result is String, so something went wrong
          case Success(error: String) =>
            val jobResult = JobResult(success = false, payload = Map.empty, request = jobCreatingRequest, errors = List(error))
            val jsonString = write(jobResult)
            mqttManager ! Publish(LymphConfig.MQTT.publishTopic, jsonString.getBytes("UTF-8").to[Vector])
            println(s"error: $error")
          // Future is not ok, definitely something went wrong
          case Failure(error: Throwable) =>
            val jobResult = JobResult(success = false, payload = Map.empty, request = jobCreatingRequest, errors = List(error.toString))
            val jsonString = write(jobResult)
            mqttManager ! Publish(LymphConfig.MQTT.publishTopic, jsonString.getBytes("UTF-8").to[Vector])
            println(s"error: $error")
        }
      }
    }

}