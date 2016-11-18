package io.hydrosphere.mist

import java.util.concurrent.Executors._

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.Cluster
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs.FullJobConfiguration

import scala.util.Random

import akka.cluster.ClusterEvent.{MemberExited, MemberRemoved, MemberUp}

import scala.concurrent.ExecutionContext
import akka.cluster.ClusterEvent._

case class TestJob()

private[mist] object CLI extends App {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)

  val cliActor = system.actorOf(Props[CLIActor], name = "CLI" )

  var argInput = ""

  if (args.length > 0) {
    println(args.foreach(_.toString))
  }

  println("Mist CLI")
  while(true) {
    print("mist>")

    val input =
      if(argInput.nonEmpty) {
        argInput
      }
      else {
        scala.io.StdIn.readLine()
      }

    argInput = ""

    println(input)

    input match {
      case "list" => {
        cliActor ! ListWorkers
      }

      case "killAll" => {
        cliActor ! StopAllContexts
      }

      case "test" => {
        cliActor ! TestJob
      }
      case "exit" => {
        system.shutdown()
        sys.exit(0)
      }
      case _@msg => {
        if(msg.contains("kill")) {
          cliActor ! new StringMessage(msg)
        }
        else {
          println(" ----------------------------------------------------------")
          println("|             Mist Command Line Interfeace                 |")
          println(" ----------------------------------------------------------")
          println("list                              List all started workers")
          println("killAll                           Stop all workers")
          println("kill <id>                         Stop worker by name")
          println("exit                              ")
          println("")
        }
      }
    }
  }
}

class CLIActor extends Actor {

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress


  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case StringMessage(message) =>
      println(message)
      if(message.contains("kill"))
        serverActor ! new RemoveContext(message.substring(5))

    case TestJob =>
      serverActor ! FullJobConfiguration(
        "/home/lblokhin/lymph/mist/examples/target/scala-2.11/mist_examples_2.11-0.0.2.jar",
        "SimpleSparkStreaming$",
        "streaming",
        Map("digits"-> (1, 2, 3, 4)),
        Option("123"))

    case StopAllContexts =>
      serverActor ! StopAllContexts

    case ListWorkers =>
      serverActor ! ListWorkers

    case MemberUp(member) =>
      if (member.address == cluster.selfAddress) {
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
