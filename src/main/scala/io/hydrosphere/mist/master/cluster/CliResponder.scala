package io.hydrosphere.mist.master.cluster

import akka.actor.{Actor, Props}
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{ListJobs, ListRoutes, ListWorkers}
import io.hydrosphere.mist.cli.{JobDescription, RouteDescription, WorkerDescription}
import io.hydrosphere.mist.master.MasterService
import io.hydrosphere.mist.utils.Logger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object CliResponder {
  
  def props(masterService: MasterService): Props =
    Props(classOf[CliResponder], masterService)
  
}

//TODO: WTF? CLINode has more methods
class CliResponder(masterService: MasterService) extends Actor with Logger {
  
  implicit val timeout = Timeout.durationToTimeout(10.seconds)

  override def receive: Receive = {
    case ListRoutes() =>
      //TODO: jobRoutes
      val description = masterService.jobRoutes.listDefinition()
        .map(d => RouteDescription(d.name, d.nameSpace, d.path, d.className))
      sender ! description

    case ListWorkers() =>
//      val originalSender = sender()
//      masterService.workers().onComplete {
//        case Success(workers) =>
//          originalSender ! workers.map(WorkerDescription(_))
//        case Failure(e) =>
//          logger.error("Error on getting workers for CLI", e)
//          originalSender ! List.empty[WorkerDescription]
//      }

    case ListJobs() =>
//      sender ! masterService.activeJobs().map(JobDescription(_))

  }
  
}
