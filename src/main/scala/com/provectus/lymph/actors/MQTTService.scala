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

private[lymph] class MQTTService extends Actor {

  context.actorOf(Manager.props(new InetSocketAddress(InetAddress.getByName(LymphConfig.MQTT.host), LymphConfig.MQTT.port))) ! Connect("MQTTService")

  override def receive: Receive = {
    case Connected =>
      println("Connected to mqtt")
      sender() ! Subscribe(Vector((LymphConfig.MQTT.subscribeTopic, AtMostOnce)), 1)
      context become ready(sender())
    case ConnectionFailure(reason) => println(s"Connection to mqtt failed [$reason]")
  }

  lazy val jobRequestActor: ActorRef = context.actorOf(Props[JobRunner], name = "AsyncJobRunner")

  def ready(mqttManager: ActorRef): Receive = {

    case Subscribed(vQoS, MessageId(1)) =>
      println("Successfully subscribed to topic foo")

    case Message(topic, payload) =>
      val stringMessage = new String(payload.to[Array], "UTF-8")
      println(s"[$topic] $stringMessage")

      val isMessageValid = JSONValidator.validate(stringMessage, JSONSchemas.asyncJobRequest)

      if (isMessageValid) {
        implicit val formats = Serialization.formats(NoTypeHints)
        val json = parse(stringMessage)
        val jobCreatingRequest = json.extract[JobConfiguration]

        // TODO: catch timeout exception
        val future = jobRequestActor.ask(jobCreatingRequest)(timeout = LymphConfig.Contexts.timeout(jobCreatingRequest.name))

        future.andThen {
          case Success(result: Map[String, Any]) =>
            val jobResult = JobResult(success = true, payload = result, request = jobCreatingRequest, errors = List.empty)
            val jsonString = write(jobResult)
            mqttManager ! Publish(LymphConfig.MQTT.publishTopic, jsonString.getBytes("UTF-8").to[Vector])
            println(s"ok, result: ${write(result)}")
          case Success(error: String) =>
            val jobResult = JobResult(success = false, payload = Map.empty, request = jobCreatingRequest, errors = List(error))
            val jsonString = write(jobResult)
            mqttManager ! Publish(LymphConfig.MQTT.publishTopic, jsonString.getBytes("UTF-8").to[Vector])
            println(s"error: $error")
          case Failure(error: Throwable) =>
            val jobResult = JobResult(success = false, payload = Map.empty, request = jobCreatingRequest, errors = List(error.toString))
            val jsonString = write(jobResult)
            mqttManager ! Publish(LymphConfig.MQTT.publishTopic, jsonString.getBytes("UTF-8").to[Vector])
            println(s"error: $error")
        }
      }
    }

}