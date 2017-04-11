package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.{Actor, ActorRef, Props}
import akka.util.Timeout
import io.hydrosphere.mist.Messages.ListRoutes
import io.hydrosphere.mist.master.JobRoutes
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.duration._

/**
  * Console interface provider
  */
class CliResponder(
  jobsRoutes: JobRoutes,
  workerManager: ActorRef
) extends Actor with Logger {
  
  implicit val timeout = Timeout.durationToTimeout(10.seconds)

  override def receive: Receive = {
    case ListRoutes =>
      sender() ! jobsRoutes.listDefinition()

    case other =>
      workerManager forward other
  }
  
}

object CliResponder {

  def props(jobRoutes: JobRoutes, workersManager: ActorRef): Props =
    Props(classOf[CliResponder], jobRoutes, workersManager)

}
