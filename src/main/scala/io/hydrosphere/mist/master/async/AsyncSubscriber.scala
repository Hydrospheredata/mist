package io.hydrosphere.mist.master.async

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.jobs.{FullJobConfigurationBuilder, JobDetails, JobResult}
import io.hydrosphere.mist.master.JobDistributor
import io.hydrosphere.mist.utils.{Logger, MultiReceiveActor}
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import spray.json.{DeserializationException, pimpString}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

private[mist] abstract class AsyncSubscriber extends Actor with MultiReceiveActor with JobConfigurationJsonSerialization with Logger {
  
  val publisherActor: ActorRef
  val provider: AsyncInterface.Provider

  private object IncomingMessageIsJobResult extends Exception
  
  receiver {
    case jobDetails: JobDetails =>
      logger.debug(s"Received JobDetails: $jobDetails")
      processJob(jobDetails)
  }

  def processIncomingMessage(message: String): Unit = {
    try {
      try {
        message.parseJson.convertTo[JobResult]
        logger.debug(s"Try to parse job result")
        throw IncomingMessageIsJobResult
      } catch {
        case _: DeserializationException => //pass
      }
      val jobCreatingRequest = FullJobConfigurationBuilder().fromJson(message).build()
      logger.info(s"Received new request: $jobCreatingRequest")
      // Run job asynchronously
      val jobDetails = JobDetails(jobCreatingRequest, JobDetails.Source.Async(provider))
      processJob(jobDetails)
    } catch {
      case _: spray.json.JsonParser.ParsingException =>
        logger.error(s"Bad JSON: $message")
      case _: DeserializationException =>
        logger.error(s"DeserializationException: Bad type in Json: $message")
      case IncomingMessageIsJobResult =>
        logger.debug("Received job result as incoming message")
      case e: Throwable =>
        logger.error(e.toString)
    }
  }
  
  def processJob(jobDetails: JobDetails): Unit = {
    val jobResult = {
      val distributorActor = context.actorOf(JobDistributor.props())
      val timeDuration = MistConfig.Contexts.timeout(jobDetails.configuration.namespace)
      if (timeDuration.isFinite()) {
        val future = distributorActor.ask(jobDetails)(timeout = FiniteDuration(timeDuration.toNanos, TimeUnit.NANOSECONDS)) recover {
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
            JobResult(success = true, payload = jobResult, request = jobDetails.configuration, errors = List.empty)
          case Right(error: String) =>
            JobResult(success = false, payload = Map.empty[String, Any], request = jobDetails.configuration, errors = List(error))
        }
      }
      else {
        distributorActor ! jobDetails
        JobResult(success = true, payload = Map("result" -> "Infinity Job Started"), request = jobDetails.configuration, errors = List.empty)
      }
    }

    publisherActor ! jobResult
  }
  
}
