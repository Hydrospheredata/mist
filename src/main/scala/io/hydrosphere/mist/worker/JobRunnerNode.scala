package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool

import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages.WorkerDidStart
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.FullJobConfiguration
import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props}
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.ExecutionContext
import scala.util.Random

class JobRunnerNode(path:String, className: String, namespace: String, externalId: String, parameters: Map[String, Any]) extends Actor with ActorLogging {

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(namespace)

  override def preStart(): Unit = {
    serverActor ! WorkerDidStart("JobStarter", cluster.selfAddress.toString)
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case MemberUp(member) =>
      if (member.address == cluster.selfAddress) {
        serverActor ! FullJobConfiguration(path, className, namespace, parameters, Option(externalId))
        cluster.system.shutdown()
      }

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }

    case MemberRemoved(member, prevStatus) =>
      if (member.address == cluster.selfAddress) {
        sys.exit(0)
      }
  }
}

object JobRunnerNode {
  def props(name: String): Props = Props(classOf[JobRunnerNode], name)
}
