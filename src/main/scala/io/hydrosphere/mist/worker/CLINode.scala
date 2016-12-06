package io.hydrosphere.mist.worker

import java.util.concurrent.Executors._

import akka.actor.{Actor, ActorRef, ActorSystem}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages.{ListMessage, RemoveContext, StopAllContexts, StringMessage}
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.ExecutionContext
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


class CLINode extends Actor {

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)
  private var senderCLI: ActorRef = _
  private val messageArray = ArrayBuffer.empty[String]


  val nodeAddress = cluster.selfAddress

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    serverActor ! new StringMessage(Constants.CLI.cliActorName + nodeAddress.toString)
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case StringMessage(message) =>
      if(message.contains(Constants.CLI.stopWorkerMsg)) {
        serverActor ! new RemoveContext(message.substring(Constants.CLI.stopWorkerMsg.length).trim)
      }
      else if(message.contains(Constants.CLI.stopJobMsg)) {
        serverActor ! new StringMessage(message)
      }
      else if(message.contains(Constants.CLI.jobMsgMarker)) {
        messageArray += message.substring(Constants.CLI.jobMsgMarker.length)
      }
      else {
        messageArray += message
      }

    case StopAllContexts =>
      serverActor ! StopAllContexts

    case ListMessage(message) =>
      senderCLI = sender()
      serverActor ! new ListMessage(message)
      context.system.scheduler.scheduleOnce(4000 millis) {
        senderCLI ! messageArray.mkString("\r\n")
        messageArray.clear()
      }

    case MemberUp(member) =>
      if (member.address == cluster.selfAddress) {
      }

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }

    case MemberRemoved(member, prevStatus) =>
      if (member.address == cluster.selfAddress) {
        cluster.down(nodeAddress)
        sys.exit(0)
      }
  }
}
