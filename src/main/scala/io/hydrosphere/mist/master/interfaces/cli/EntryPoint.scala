package io.hydrosphere.mist.master.interfaces.cli

import akka.actor.ActorSystem
import akka.util.Timeout
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.{implicitConversions, postfixOps}

object EntryPoint extends App {

  val masterAddress = args(0)

  implicit val timeout = Timeout.durationToTimeout(Constants.CLI.timeoutDuration)
  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)

  private val responder = {
    val address = s"akka.tcp://$masterAddress/user/${Constants.Actors.cliResponderName}"
    val resolve = system.actorSelection(address).resolveOne()
    Await.result(resolve, Duration.Inf)
  }

  var argInput = args.drop(1).mkString(" ")

  if(argInput.isEmpty) {
    println("Hello! This is a Mist command-line interface.")
    println("Enter your command please.")
  }

  while(true) {
    val input =
      if(argInput.nonEmpty) {
        println(argInput)
        argInput
      }
      else {
        print("mist>")
        readLine()
      }

    argInput = if (argInput.nonEmpty) "exit" else ""

    val parsed = Command.parse(input)
    parsed.foreach({
      case remote: RemoteCliCommand[_] => remote.exec(responder)
      case Exit =>
        system.shutdown
        sys.exit(0)
      case Help =>
        printHelp()
    })

    if (parsed.isEmpty) {
      printHelp()
    }

  }

  private def printHelp(): Unit = {
    println(s" ----------------------------------------------------------------- \n" +
      s"|             Mist Command Line Interface                          | \n" +
      s" ----------------------------------------------------------------- \n" +
      s"${Constants.CLI.Commands.help} \t print help \n" +
      s"${Constants.CLI.Commands.startJob} <router> <extId> \t start job \n" +
      s"${Constants.CLI.Commands.listWorkers} \t \t \t \t List all started workers \n" +
      s"${Constants.CLI.Commands.listJobs} \t \t \t \t List all started jobs \n" +
      s"${Constants.CLI.Commands.listRouters} \t \t \t \t List routers \n" +
      s"${Constants.CLI.Commands.stopAllWorkers} \t \t \t \t Stop all workers \n" +
      s"${Constants.CLI.Commands.stopWorker} <namespace> \t \t Stop worker by UID \n" +
      s"${Constants.CLI.Commands.stopJob} <extId|UID> \t \t \t Stop job by external id or UID\n" +
      s"${Constants.CLI.Commands.exit} \t \n")

  }
}
