package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool

import akka.actor.{Actor, ActorLogging, Address, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.jobs.{JobExecutionParams, JobDetails}
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Random

class JobRunnerNode(jobRequest: JobExecutionParams) extends Actor with ActorLogging {

  val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.clusterManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress: Address = cluster.selfAddress

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    // TODO: train|serve
    case MemberUp(member) =>
      if (member.address == cluster.selfAddress) {
        serverActor ! JobDetails(jobRequest, JobDetails.Source.Cli)
        cluster.system.shutdown()
      }

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }

    case MemberRemoved(member, _) =>
      if (member.address == cluster.selfAddress) {
        sys.exit(0)
      }
  }
}

object JobRunnerNode {
  def props(jobConfiguration: JobExecutionParams): Props = Props(classOf[JobRunnerNode], jobConfiguration)
}
