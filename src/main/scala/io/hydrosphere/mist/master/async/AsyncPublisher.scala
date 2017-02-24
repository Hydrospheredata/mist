package io.hydrosphere.mist.master.async

import akka.actor.Actor
import io.hydrosphere.mist.jobs.JobResult
import org.json4s.DefaultFormats
import org.json4s.native.Json

private[mist] trait AsyncPublisher extends Actor {
  override def receive: Receive = {
    case jobResult: JobResult =>
      val jsonString = Json(DefaultFormats).write(jobResult)
      send(jsonString)
    case string: String =>
      send(string)
  }

  def send(message: String): Unit
}
