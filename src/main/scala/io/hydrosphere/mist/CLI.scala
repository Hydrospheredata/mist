package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props, _}
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.worker.CLINode

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

private[mist] object CLI extends App {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
  val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName )

  var argInput = args.mkString(" ")
  implicit val timeout = Timeout(5 seconds)

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
      case msg if msg.contains(Constants.CLI.listWorkersMsg) || msg.contains(Constants.CLI.listJobsMsg) => {
        val future = cliActor ? new ListMessage(msg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
      case msg if msg.contains(Constants.CLI.stopWorkerMsg) || msg.contains(Constants.CLI.stopJobMsg) => {
        cliActor ! new StringMessage(msg)
        val future = cliActor ? new ListMessage(msg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
      case msg@Constants.CLI.stopAllWorkersMsg => {
        cliActor ! StopAllContexts
        val future = cliActor ? new ListMessage(msg)
        val result = Await.result(future, timeout.duration).asInstanceOf[String]
        println(result)
      }
      case msg@Constants.CLI.exitMsg => {
        system.shutdown
        sys.exit(0)
      }
      case _ => {
        println(s" ---------------------------------------------------------- \n" +
          s"|             Mist Command Line Interface                  | \n" +
          s"---------------------------------------------------------- \n" +
          s"${Constants.CLI.listWorkersMsg} \t \t List all started workers \n" +
          s"${Constants.CLI.listJobsMsg} \t \t List all started jobs \n" +
          s"${Constants.CLI.stopAllWorkersMsg} \t \t Stop all workers \n" +
          s"${Constants.CLI.stopWorkerMsg} <name> \t Stop worker by name \n" +
          s"${Constants.CLI.stopJobMsg} <extId> \t Stop job by external id \n" +
          s"${Constants.CLI.exitMsg} \t \n")
      }
    }

  }
}


