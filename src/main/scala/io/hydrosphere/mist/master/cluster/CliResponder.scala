package io.hydrosphere.mist.master.cluster

import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages.{ListJobs, ListRoutes, ListWorkers}
import io.hydrosphere.mist.{Constants, RouteConfig}
import io.hydrosphere.mist.cli.{JobDescription, RouteDescription, WorkerDescription}
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.jobs.store.JobRepository
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.Logger

import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

private[mist] object CliResponder {
  
  def props(): Props = Props(classOf[CliResponder])
  
}

private[mist] class CliResponder extends Actor with Logger {
  
  private implicit val timeout = Timeout.durationToTimeout(10.seconds)

  override def receive: Receive = {
    case ListRoutes() =>
      logger.debug("Received ListRouters")
      sender ! RouteConfig.routes.map {
        case (key, value) =>
          val routeMap = value.asInstanceOf[Map[String, String]]
          key -> RouteDescription(key, routeMap("namespace"), routeMap("path"), routeMap("className"))
      }.values.toList

    case ListWorkers() =>
      val originalSender = sender()
      val future = context.system.actorSelection(s"akka://mist/user/${Constants.Actors.clusterManagerName}") ? ClusterManager.GetWorkers()
      future onComplete {
        case Success(value) =>
          logger.debug(s"SUCCESS: $value")
          originalSender ! value.asInstanceOf[List[WorkerLink]].map(WorkerDescription(_))
        case Failure(exc) =>
          logger.debug(s"FAILURE: $exc")
          originalSender ! List.empty[WorkerDescription]
      }

    case ListJobs() =>
      logger.debug("Received ListJobs")
      
      sender ! JobRepository().filteredByStatuses(List(JobDetails.Status.Running, JobDetails.Status.Queued)).map {
        job => JobDescription(job)
      }

  }
  
}
