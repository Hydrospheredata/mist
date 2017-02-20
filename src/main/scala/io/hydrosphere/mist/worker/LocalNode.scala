package io.hydrosphere.mist.worker

import akka.actor.{Actor, ActorLogging}
import io.hydrosphere.mist.jobs.{JobDetails, ServingJobConfiguration}
import io.hydrosphere.mist.jobs.runners.Runner

class LocalNode extends Actor with ActorLogging {
  override def receive: Receive = {
    case jobRequest: JobDetails =>
      val originalSender = sender
      try {
        val runner = Runner(jobRequest, null)
        val result = runner.run()
        originalSender ! result
        
      } catch {
        case exc: Throwable => originalSender ! Right(exc.toString)
      }
  }
}
