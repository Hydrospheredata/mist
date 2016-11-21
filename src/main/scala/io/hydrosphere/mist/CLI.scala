package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.worker.CLINode
import io.hydrosphere.mist.jobs.FullJobConfiguration

import scala._
import io._

private[mist] object CLI extends App {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)

  val cliActor = system.actorOf(Props[CLINode], name = "CLI" )

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
        readLine()
      }

    argInput = ""

    input match {

      case "list" => {
        cliActor ! ListWorkers
      }

      case "killAll" => {
        cliActor ! StopAllContexts
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
          println("|             Mist Command Line Interface                  |")
          println(" ----------------------------------------------------------")
          println("list                              List all started workers")
          println("killAll                           Stop all workers")
          println("kill <name>                       Stop worker by name")
          println("exit                              ")
          println("")
        }
      }
    }
  }
}


