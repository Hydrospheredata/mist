package io.hydrosphere.mist.master.async

import java.util.concurrent.TimeUnit

import akka.actor.Actor
import akka.pattern.ask
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.{FullJobConfigurationBuilder, JobDetails, JobResult}
import io.hydrosphere.mist.master.JobDistributor
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import org.json4s.DefaultFormats
import org.json4s.native.Json
import spray.json.{DeserializationException, pimpString}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

trait AsyncServiceActor extends Actor with JobConfigurationJsonSerialization with Logger {

  private object IncomingMessageIsJobResult extends Exception

  def processIncomingMessage(message: String): Unit = {
    val jobResult = try {
      try {
        message.parseJson.convertTo[JobResult]
        logger.debug(s"Try to parse job result")
        throw IncomingMessageIsJobResult
      } catch {
        case _: DeserializationException => //pass
      }
      val jobCreatingRequest = FullJobConfigurationBuilder().fromJson(message).build()
      logger.info(s"Received new request: $jobCreatingRequest")
//      val workerManagerActor = context.system.actorSelection(s"akka://mist/user/${Constants.Actors.clusterManagerName}")
      val distributorActor = context.system.actorOf(JobDistributor.props())
      // Run job asynchronously

      val timeDuration = MistConfig.Contexts.timeout(jobCreatingRequest.namespace)
      if (timeDuration.isFinite()) {
        val future = distributorActor.ask(jobCreatingRequest)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS)) recover {
          case error: Throwable => Right(error.toString)
        }
        val result = Await.ready(future, Duration.Inf).value.get
        val jobResult = result match {
          case Success(r: JobDetails) => r.jobResult.getOrElse(Left("Empty result"))
          case Success(r: Either[String, Map[String, Any]]) => r
          case Failure(r) => r
        }

        jobResult match {
          case Left(jobResult: Map[String, Any]) =>
            JobResult(success = true, payload = jobResult, request = jobCreatingRequest, errors = List.empty)
          case Right(error: String) =>
            JobResult(success = false, payload = Map.empty[String, Any], request = jobCreatingRequest, errors = List(error))
        }
      }
      else {
        distributorActor ! jobCreatingRequest
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
//        logger.error(e.toString)
        throw e
        null
    }

    if (jobResult != null) {
      val jsonString = Json(DefaultFormats).write(jobResult)
      send(jsonString)
    }
  }
  
  def send(message: String): Unit

}
