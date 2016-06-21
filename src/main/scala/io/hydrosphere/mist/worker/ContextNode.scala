package io.hydrosphere.mist.worker

import akka.cluster.ClusterEvent.{UnreachableMember, MemberEvent, InitialStateAsEvents}
import io.hydrosphere.mist.actors.tools.Messages.CreateContext

import collection.JavaConversions._

import akka.cluster.Cluster
import akka.actor.{ActorLogging, Actor}

import io.hydrosphere.mist.MistConfig

import scala.util.Random

class ContextNode extends Actor with ActorLogging{

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/clusterMist"
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  override def preStart() {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop() {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case CreateContext(name) =>
      println(s"I should create new contex `$name`")
  }
}
