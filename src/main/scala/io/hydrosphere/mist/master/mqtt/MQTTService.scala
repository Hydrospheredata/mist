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

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}


private[mist] case object MqttSubscribe

private[mist] trait MqttPubSubActor { this: Actor =>
  val pubsub = context.actorOf(Props(classOf[MqttPubSub], s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}"))
}

private[mist] class MQTTServiceActor extends Actor with MqttPubSubActor with JsonFormatSupport with Logger{

  private def wrapError(error: String, jobCreatingRequest: JobConfiguration): JobResult = {
    JobResult(success = false, payload = Map.empty[String, Any], request = jobCreatingRequest, errors = List(error))
  }

  def receive: Receive = {

    case MqttSubscribe => pubsub ! MqttPubSub.Subscribe(self)

    case msg @ MqttPubSub.SubscribeAck(MqttPubSub.Subscribe(`self`)) => context become ready
  }

  def ready: Receive = {

    case msg: MqttPubSub.Message =>

      val stringMessage = new String(msg.payload, "utf-8")

      logger.info("Receiving Data, Topic : %s, Message : %s".format(MistConfig.MQTT.publishTopic, stringMessage))

      val jobResult = try {
        val json = stringMessage.parseJson
        // map request into JobConfiguration
        implicit val jobCreatingRequest = json.convertTo[JobConfiguration]

        val workerManagerActor = context.system.actorSelection(s"akka://mist/user/${Constants.Actors.workerManagerName}")
        // Run job asynchronously
        val future = workerManagerActor.ask(jobCreatingRequest)(timeout = MistConfig.Contexts.timeout(jobCreatingRequest.name)) recover {
          case error: Throwable => Right(error.toString)
        }

        val result = Await.ready(future, Duration.Inf).value.get
        val jobResultEither = result match {
          case Success(r) => r
          case Failure(r) => r
        }

        jobResultEither match {
          case Left(jobResult: Map[String, Any]) =>
            JobResult(success = true, payload = jobResult, request = jobCreatingRequest, errors = List.empty)
          case Right(error: String) =>
            wrapError(error, jobCreatingRequest)
        }

      } catch {
        case _: spray.json.JsonParser.ParsingException =>
          logger.error(s"Bad JSON: $stringMessage")
          wrapError("Bad JSON", jobCreatingRequest = JobConfiguration("", "", "", Map.empty))
        case _: DeserializationException =>
          logger.error(s"DeserializationException Bad type in Json: $stringMessage")
          wrapError("DeserializationException: Bad type in Json", jobCreatingRequest = JobConfiguration("", "", "", Map.empty))
        case e: Throwable =>
          wrapError(e.toString, jobCreatingRequest = JobConfiguration("", "", "", Map.empty))
      }

      val jsonString = Json(DefaultFormats).write(jobResult)
      pubsub ! new MqttPubSub.Publish(jsonString.getBytes("utf-8"))

  }
}
