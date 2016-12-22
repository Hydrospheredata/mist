package io.hydrosphere.mist.master.mqtt

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.{Constants, Logger, MistConfig, RouteConfig}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json.{DeserializationException, pimpString}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success}


private[mist] case object MQTTSubscribe

private[mist] trait MQTTPubSubActor { this: Actor =>
  val pubsub: ActorRef = context.actorOf(Props(classOf[MQTTPubSub], s"tcp://${MistConfig.MQTT.host}:${MistConfig.MQTT.port}"))
}

private[mist] class MQTTServiceActor extends Actor with MQTTPubSubActor with JobConfigurationJsonSerialization with Logger{

  private object IncomingMessageIsJobRequest extends Exception

  private def wrapError(error: String, jobCreatingRequest: FullJobConfiguration): JobResult = {
    JobResult(success = false, payload = Map.empty[String, Any], request = jobCreatingRequest, errors = List(error))
  }

  def receive: Receive = {

    case MQTTSubscribe => pubsub ! MQTTPubSub.Subscribe(self)

    case _ @ MQTTPubSub.SubscribeAck(MQTTPubSub.Subscribe(`self`)) => context become ready
  }

  def ready: Receive = {

    case msg: MQTTPubSub.Message =>

      val stringMessage = new String(msg.payload, "utf-8")

      logger.info("Receiving Data, Topic : %s, Message : %s".format(MistConfig.MQTT.publishTopic, stringMessage))

      val jobResult = try {
        val json = stringMessage.parseJson
        // map request into JobConfiguration
        // TODO: training/serve 
        val jobCreatingRequest = try {
          json.convertTo[MistJobConfiguration]
        } catch {
          case _: DeserializationException =>
            logger.debug(s"Try to parse restificated request")
            val restificatedRequest = try {
              json.convertTo[RestificatedMistJobConfiguration]
            } catch {
              case _: DeserializationException =>
                logger.debug(s"Try to parse job result")
                json.convertTo[JobResult]
                throw IncomingMessageIsJobRequest
            }
            val config = RouteConfig(restificatedRequest.route)
            MistJobConfiguration(config.path, config.className, config.namespace, restificatedRequest.parameters, restificatedRequest.externalId)
        }

        val workerManagerActor = context.system.actorSelection(s"akka://mist/user/${Constants.Actors.workerManagerName}")
        // Run job asynchronously

        val timeDuration = MistConfig.Contexts.timeout(jobCreatingRequest.namespace)
        if(timeDuration.isFinite()) {
          val future = workerManagerActor.ask(jobCreatingRequest)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS)) recover {
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
        }
        else {
          workerManagerActor ! jobCreatingRequest
          JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobCreatingRequest, errors = List.empty)
        }

      } catch {
        case _: spray.json.JsonParser.ParsingException =>
          logger.error(s"Bad JSON: $stringMessage")
          wrapError("Bad JSON", jobCreatingRequest = MistJobConfiguration("", "", "", Map.empty))
        case _: DeserializationException =>
          logger.error(s"DeserializationException: Bad type in Json: $stringMessage")
          wrapError("DeserializationException: Bad type in Json", jobCreatingRequest = MistJobConfiguration("", "", "", Map.empty))
        case IncomingMessageIsJobRequest =>
          logger.debug("Received job result as incoming message")
          null
        case e: Throwable =>
          wrapError(e.toString, jobCreatingRequest = MistJobConfiguration("", "", "", Map.empty))
      }

      if (jobResult != null) {
        val jsonString = Json(DefaultFormats).write(jobResult)
        pubsub ! new MQTTPubSub.Publish(jsonString.getBytes("utf-8"))
      }

  }
}
