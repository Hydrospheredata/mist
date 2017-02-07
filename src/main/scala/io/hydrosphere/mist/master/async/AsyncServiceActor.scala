package io.hydrosphere.mist.master.async

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.pattern.ask
import io.hydrosphere.mist.{Constants, MistConfig}
import io.hydrosphere.mist.jobs.{FullJobConfigurationBuilder, JobResult}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json.{DeserializationException, pimpString}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success}

trait AsyncServiceActor extends Actor with JobConfigurationJsonSerialization with Logger {

  private object IncomingMessageIsJobResult extends Exception

  def processIncomingMessage(message: String): Unit = {
    val jobResult = try {
      try {
        message.parseJson.convertTo[JobResult]
        logger.debug(s"Try to parse job result")
      } catch {
        case _: DeserializationException =>
          throw IncomingMessageIsJobResult
      }
      val jobCreatingRequest = FullJobConfigurationBuilder().fromJson(message).build()

      val workerManagerActor = context.system.actorSelection(s"akka://mist/user/${Constants.Actors.clusterManagerName}")
      // Run job asynchronously

      val timeDuration = MistConfig.Contexts.timeout(jobCreatingRequest.namespace)
      if (timeDuration.isFinite()) {
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
            JobResult(success = false, payload = Map.empty[String, Any], request = jobCreatingRequest, errors = List(error))
        }
      }
      else {
        workerManagerActor ! jobCreatingRequest
        JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobCreatingRequest, errors = List.empty)
      }

    } catch {
      case _: spray.json.JsonParser.ParsingException =>
        logger.error(s"Bad JSON: $message")
        null
      case _: DeserializationException =>
        logger.error(s"DeserializationException: Bad type in Json: $message")
        null
      case IncomingMessageIsJobResult =>
        logger.debug("Received job result as incoming message")
        null
      case e: Throwable =>
        logger.error(e.toString)
        null
    }

    if (jobResult != null) {
      val jsonString = Json(DefaultFormats).write(jobResult)
      send(jsonString)
    }
  }
  
  def send(message: String): Unit

}
