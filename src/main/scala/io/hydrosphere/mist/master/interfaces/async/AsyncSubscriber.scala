package io.hydrosphere.mist.master.interfaces.async

import akka.actor.{Actor, ActorRef}
import io.hydrosphere.mist.jobs.JobExecutionRequest
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.utils.{Logger, MultiReceivable}
import spray.json.pimpString

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

abstract class AsyncSubscriber(masterService: MasterService) extends Actor
  with MultiReceivable
  with JobConfigurationJsonSerialization
  with Logger {
  
  val publisherActor: ActorRef
  val provider: AsyncInterface.Provider

  def processIncomingMessage(message: String): Unit = {
    extractRequest(message) match {
      case Some(req) =>
        val future = masterService.startJob(req)
        future.onComplete({
          case Success(result) => publisherActor ! result
          case Failure(e) =>
            logger.error(s"Job $req execution failed", e)
            val msg = s"Job execution failed for $req. Error message ${e.getMessage}"
            publisherActor ! msg
        })

      case None =>
    }
  }


  private def extractRequest(message: String): Option[JobExecutionRequest] = {
    try {
      val json = message.parseJson
      val fields = json.asJsObject.fields
      val hasAllFields = Seq("jobId", "parameters", "action").map(fields.contains).reduce(_ && _)
      if (hasAllFields) {
        val params = json.convertTo[JobExecutionRequest]
        Some(params)
      } else {
        logger.trace(s"Incoming message is not supported $message")
        None
      }
    } catch {
      case e: Throwable =>
        logger.trace(s"Received invalid message $message", e)
        None

    }
  }
  

}
