package io.hydrosphere.mist.master

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, Props}
import io.hydrosphere.mist.{Constants, MistConfig}
import io.hydrosphere.mist.jobs.{JobConfiguration, JobDetails}
import io.hydrosphere.mist.master.cluster.ClusterManager.GetWorker
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.LocalNode

private[mist] object JobManager {
  
  case class StartJob(jobDetails: JobDetails)
  
  def props(): Props = Props(classOf[JobManager])
  
}

private[mist] class JobManager extends Actor with Logger {
  
  override def receive: Receive = {
    case JobManager.StartJob(jobDetails) if jobDetails.configuration.action == JobConfiguration.Action.Serve =>
      val localNodeActor = context.actorOf(Props(classOf[LocalNode]))
      localNodeActor ! jobDetails
      context become gettingResults(sender)

    case JobManager.StartJob(jobDetails) =>
      context.system.actorSelection(s"akka://mist/user/${Constants.Actors.clusterManagerName}") ! GetWorker(jobDetails.configuration.namespace)
      context become ready(jobDetails, sender())
  }
  
  def ready(jobDetails: JobDetails, originalSender: ActorRef): Receive = {
    case WorkerLink(_, name, address, _) =>
      val remoteActor = context.system.actorSelection(s"$address/user/$name")
      remoteActor ! jobDetails
      context become gettingResults(originalSender)
  }
  
  def gettingResults(originalSender: ActorRef): Receive = {
    case jobDetails: JobDetails =>
      originalSender ! jobDetails
  }
}
