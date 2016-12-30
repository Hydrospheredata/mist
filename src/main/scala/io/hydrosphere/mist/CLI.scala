package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.worker.{CLINode, JobDescriptionSerializable, WorkerDescription}

import scala.concurrent.Await
import scala.language.postfixOps
import scala.sys.process._

private[mist] object CLI extends App {

  implicit val system = ActorSystem("mist", MistConfig.Akka.CLI.settings)
  val cliActor = system.actorOf(Props[CLINode], name = Constants.CLI.cliActorName )

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

    argInput =
      if(argInput.nonEmpty) {
        "exit"
      }
      else
      {
        ""
      }

    def beautifulPrintResult(header: List[String] = List())(someList: List[Any]): Unit = {
      if(someList.nonEmpty) {
        val headerTabs = {
          try {
            someList.asInstanceOf[List[WorkerDescription]].head.length()
          } catch {
            case _: ClassCastException => {
              try {
                someList.asInstanceOf[List[JobDescriptionSerializable]].head.length()
              } catch {
                case _: ClassCastException => List[Int](0)
              }
            }
          }
        }

        (header zip headerTabs).foreach { case (h, t) => print(h + " " * (t - h.length) + "\t") }
        print("\n")
        someList.foreach(y => println(y.toString))
      }
      else {
        header foreach (h => print(h + "\t"))
        print("\n")
      }
    }

    def cliResponseBuilder[A](msg: A, out: (List[Any]) => Unit): Unit = {
      implicit def anyToListAny(a: Any): List[Any] = if(a.isInstanceOf[List[Any]]) a.asInstanceOf[List[Any]] else List[Any](a)
      val future = cliActor.ask(msg)(timeout = Constants.CLI.timeoutDuration)
      val result = Await.result(future, Constants.CLI.timeoutDuration)
      out(result)
    }

    input match {
      case msg if msg.contains(Constants.CLI.listJobsMsg) => {
        val header = List("UID","TIME","NAMESPACE","EXT_ID","tROUTER")
        cliResponseBuilder(ListJobs, beautifulPrintResult(header))
      }

      case msg if msg.contains(Constants.CLI.listWorkersMsg) => {
        val header = List("NAMESPACE", "ADDRESS")
        cliResponseBuilder(ListWorkers, beautifulPrintResult(header))
      }

      case msg if msg.contains(Constants.CLI.listRoutersMsg) => {
        cliResponseBuilder(ListRouters, beautifulPrintResult())
      }

      case msg if msg.contains(Constants.CLI.stopWorkerMsg) => {
        cliResponseBuilder(new StopWorker(msg), beautifulPrintResult())
      }

      case msg if msg.contains(Constants.CLI.stopJobMsg) => {
        cliResponseBuilder(new StopJob(msg), beautifulPrintResult())
      }

      case msg if msg.contains(Constants.CLI.stopAllWorkersMsg) => {
        cliResponseBuilder(StopAllContexts, beautifulPrintResult())
      }

      case msg if msg.contains(Constants.CLI.startJob) => {
        val listCmd = msg.substring(Constants.CLI.startJob.length).trim.split(' ')
        if(listCmd.size == 3) {
          val config = "--config " + listCmd(0)
          val route = "--route " + listCmd(1)
          val externalId = "--external-id " + listCmd(2)
          s"bin/mist start job ${config} ${route} ${externalId}".!
        }
        else {
          println(listCmd.mkString(" "))
        }
      }

      case msg@Constants.CLI.exitMsg => {
        system.shutdown
        sys.exit(0)
      }
      case _ => {
        println(s" ----------------------------------------------------------------- \n" +
          s"|             Mist Command Line Interface                          | \n" +
          s" ----------------------------------------------------------------- \n" +
          s"${Constants.CLI.startJob} <config> <router> <extId> \t start job \n" +
          s"${Constants.CLI.listWorkersMsg} \t \t \t \t List all started workers \n" +
          s"${Constants.CLI.listJobsMsg} \t \t \t \t List all started jobs \n" +
          s"${Constants.CLI.listRoutersMsg} \t \t \t \t List routers \n" +
          s"${Constants.CLI.stopAllWorkersMsg} \t \t \t \t Stop all workers \n" +
          s"${Constants.CLI.stopWorkerMsg} <namespace> \t \t Stop worker by namespace \n" +
          s"${Constants.CLI.stopJobMsg} <extId|UID> \t \t \t Stop job by external id or UID\n" +
          s"${Constants.CLI.exitMsg} \t \n")
      }
    }
  }
}
