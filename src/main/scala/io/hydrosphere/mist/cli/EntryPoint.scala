package io.hydrosphere.mist.cli

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.Await
import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._


private[mist] object EntryPoint extends App {

  implicit val timeout = Timeout.durationToTimeout(Constants.CLI.timeoutDuration)
  implicit val system = ActorSystem("mist-cli", MistConfig.Akka.CLI.settings)
  val cliActor = system.actorOf(Props[CLINode], name = Constants.Actors.cliName )

  var argInput = args.mkString(" ")

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
    
    def prettyPrint[A <: Description](headers: List[String])(list: List[A]): Unit = {
      
      def printBorder(sizes: List[Int]): Unit = {
        print("+")
        sizes.foreach({ i => print("-" * i + "+") })
        print("\n")
      }
      
      def printRow(sizes: List[Int], data: List[String]) = {
        print("|")
        data.zipWithIndex.foreach({
          case (string, idx) => print(" " + string + " " * (sizes(idx) - string.length - 1) + "|")
        })
        print("\n")
      }
      
      val columnSizes = (list.map(_.fields) :+ headers).transpose.map(_.map((_: String).length).max + 2)

      printBorder(columnSizes)
      printRow(columnSizes, headers)
      printBorder(columnSizes)
      list.foreach({
        row => printRow(columnSizes, row.fields)
      })
      printBorder(columnSizes)
    }
    
    input match {
      case msg if msg.contains(Constants.CLI.Commands.listJobs) =>
        val future = cliActor ? ListJobs()
        val result = Await.result(future, Constants.CLI.timeoutDuration).asInstanceOf[List[JobDescription]]
        prettyPrint(JobDescription.headers)(result)

      case msg if msg.contains(Constants.CLI.Commands.listWorkers) =>
        val future = cliActor ? ListWorkers()
        val result = Await.result(future, Constants.CLI.timeoutDuration).asInstanceOf[List[WorkerDescription]]
        prettyPrint(WorkerDescription.headers)(result)

      case msg if msg.contains(Constants.CLI.Commands.listRouters) =>
        val future = cliActor.ask(ListRoutes())(timeout = Constants.CLI.timeoutDuration)
        val result = Await.result(future, Constants.CLI.timeoutDuration).asInstanceOf[List[RouteDescription]]
        prettyPrint(RouteDescription.headers)(result)

      case msg if msg.contains(Constants.CLI.Commands.stopWorker) =>
        val uid = msg.substring(Constants.CLI.Commands.stopWorker.length).trim
        val future = cliActor ? StopWorker(uid)
        val result = Await.result(future, Constants.CLI.timeoutDuration).asInstanceOf[String]
        println(result)

      case msg if msg.contains(Constants.CLI.Commands.stopJob) =>
        val uid = msg.substring(Constants.CLI.Commands.stopJob.length).trim
        val future = cliActor ? StopJob(uid)
        val result = Await.result(future, Constants.CLI.timeoutDuration).asInstanceOf[String]
        println(result)

      case msg if msg.contains(Constants.CLI.Commands.stopAllWorkers) =>
        val future = cliActor ? StopAllWorkers()
        val result = Await.result(future, Constants.CLI.timeoutDuration).asInstanceOf[String]
        println(result)

      case msg if msg.contains(Constants.CLI.Commands.startJob) =>
        val listCmd = msg.substring(Constants.CLI.Commands.startJob.length).trim.split(' ')
        if(listCmd.length == 3) {
          val config = "--config " + listCmd(0)
          val route = "--route " + listCmd(1)
          val externalId = "--external-id " + listCmd(2)
          s"bin/mist start job $config $route $externalId".!
        }
        else {
          println(listCmd.mkString(" "))
        }

      case Constants.CLI.Commands.exit =>
        system.shutdown
        sys.exit(0)
      case _ =>
        println(s" ----------------------------------------------------------------- \n" +
          s"|             Mist Command Line Interface                          | \n" +
          s" ----------------------------------------------------------------- \n" +
          s"${Constants.CLI.Commands.startJob} <config> <router> <extId> \t start job \n" +
          s"${Constants.CLI.Commands.listWorkers} \t \t \t \t List all started workers \n" +
          s"${Constants.CLI.Commands.listJobs} \t \t \t \t List all started jobs \n" +
          s"${Constants.CLI.Commands.listRouters} \t \t \t \t List routers \n" +
          s"${Constants.CLI.Commands.stopAllWorkers} \t \t \t \t Stop all workers \n" +
          s"${Constants.CLI.Commands.stopWorker} <namespace> \t \t Stop worker by UID \n" +
          s"${Constants.CLI.Commands.stopJob} <extId|UID> \t \t \t Stop job by external id or UID\n" +
          s"${Constants.CLI.Commands.exit} \t \n")
    }
  }
}
