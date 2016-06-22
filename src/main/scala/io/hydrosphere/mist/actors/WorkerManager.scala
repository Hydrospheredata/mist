package io.hydrosphere.mist.actors

import akka.actor.Actor
import akka.pattern.ask
import akka.cluster.ClusterEvent.{MemberUp, MemberEvent, UnreachableMember, InitialStateAsEvents}
import akka.cluster._
import scala.concurrent.duration._
import io.hydrosphere.mist.actors.tools.Messages.{WorkerDidStart, RemoveContext, CreateContext, StopAllContexts}
import io.hydrosphere.mist.jobs.JobConfiguration
import sys.process._
import scala.concurrent.ExecutionContext.Implicits.global


/** Manages context repository */
private[mist] class WorkerManager extends Actor {

  private val cluster = Cluster(context.system)
  private val clusterState = cluster.state

  private val myAddress = cluster.selfAddress

  private val workers = scala.collection.mutable.Map[String, String]()

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

    case WorkerDidStart(name, address) =>
      println(s"Worker `$name` did start on $address")
      workers += (name -> address)

    case jobRequest: JobConfiguration =>
      val originalSender = sender
      val remoteActor = cluster.system.actorSelection(workers(jobRequest.name))

//      implicit val timeout = 1000.days
      val future = remoteActor.ask(jobRequest)(timeout = 248.days)
      future.andThen {
        case response => originalSender ! response
      }
  }
}
