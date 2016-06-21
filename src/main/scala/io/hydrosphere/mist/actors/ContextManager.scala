package io.hydrosphere.mist.actors

import akka.actor.Actor
import akka.cluster.ClusterEvent.{MemberUp, MemberEvent, UnreachableMember, InitialStateAsEvents}
import akka.cluster._
import io.hydrosphere.mist.actors.tools.Messages.{RemoveContext, CreateContext, StopAllContexts}
import sys.process._


/** Manages context repository */
private[mist] class ContextManager extends Actor {

  private val cluster = Cluster(context.system)
  private val clusterState = cluster.state

  private val myAddress = cluster.selfAddress

  override def preStart() {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop() {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case CreateContext(name) =>
      // TODO: do not start already started member for `name`
      val thread = new Thread {
        override def run(): Unit = {
          sys.env("MIST_HOME") + "/mist.sh worker --namespace " + name !
        }
      }
      thread.start()

    // surprise: stops all contexts
    case StopAllContexts =>
      // TODO: remove cluster members, shut down JVMs

    // removes context
    case RemoveContext(contextWrapper) =>
    // TODO: remove cluster member by name, shut down JVM

    case MemberUp(member) =>
      //TODO: associate member with context name
      println(s"[Listener] node is up: $member, ${member.getRoles}, ${member.uniqueAddress}")

  }
}
