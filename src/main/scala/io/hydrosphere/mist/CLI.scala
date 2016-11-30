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

    val input =
      if(argInput.nonEmpty) {
        argInput
      }
      else {
        Thread.sleep(1000)
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

      case "list" => {
        cliActor ! ListMessage
      }

      case "killAll" => {
        cliActor ! StopAllContexts
      }
      case "exit" => {
        system.shutdown()
        sys.exit(0)
      }
      case _@msg => {
        if(msg.contains("kill") || msg.contains("stop")) {
          cliActor ! new StringMessage(msg)
        }
        else {
          println(" ----------------------------------------------------------")
          println("|             Mist Command Line Interface                  |")
          println(" ----------------------------------------------------------")
          println("list                              List all started workers")
          println("killAll                           Stop all workers")
          println("kill <name>                       Stop worker by name")
          println("stop <extId>                      Stop job by external id")
          println("exit                              ")
          println("")
        }
      }
    }
  }
}


