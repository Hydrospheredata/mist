package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.worker.CLINode
import io.hydrosphere.mist.jobs.FullJobConfiguration
import io.hydrosphere.mist.Constants

import scala._
import io._

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

private[mist] object CLI extends App {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)

  val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName )

  var argInput = args.mkString(" ")

  println("Mist CLI")
  while(true) {

    val input =
      if(argInput.nonEmpty) {
        argInput
      }
      else {
        print("mist>")
        readLine()
      }

    argInput =
        if(argInput.nonEmpty) {
            "exit"
        }
        else
        {
            ""
        }

    input match {
      case msg if(msg.contains(Constants.CLI.listWorkersMsg) || msg.contains(Constants.CLI.listJobsMsg)) => {
        //cliActor ! new ListMessage(msg)
        implicit val timeout = Timeout(5 seconds)
        val future = cliActor ? new ListMessage(msg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
      case msg if(msg.contains(Constants.CLI.stopWorkerMsg)|| msg.contains(Constants.CLI.stopJobMsg)) => {
        cliActor ! new StringMessage(msg)
        implicit val timeout = Timeout(5 seconds)
        val future = cliActor ? new ListMessage(msg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
      case msg if(msg.contains(Constants.CLI.stopAllWorkersMsg)) => {
        cliActor ! StopAllContexts
        implicit val timeout = Timeout(5 seconds)
        val future = cliActor ? new ListMessage(msg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
      case msg if(msg.contains(Constants.CLI.exitMsg)) => {
        system.shutdown
        sys.exit(0)
      }
      case _ => {
        println(" ----------------------------------------------------------")
        println("|             Mist Command Line Interface                  |")
        println(" ----------------------------------------------------------")
        println(Constants.CLI.listWorkersMsg + "\t List all started workers")
        println(Constants.CLI.listJobsMsg + "\t List all started jobs")
        println(Constants.CLI.stopAllWorkersMsg + "\t Stop all workers")
        println(Constants.CLI.stopWorkerMsg + " <name>\t Stop worker by name")
        println(Constants.CLI.stopJobMsg + " <extId>\t Stop job by external id")
        println(Constants.CLI.exitMsg + "\t ")
        println("")
      }
    }

  }
}


