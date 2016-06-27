package io.hydrosphere.mist.master

import java.io.File

import akka.actor.{AddressFromURIString, Actor}
import akka.pattern.ask
import akka.cluster.Cluster
import io.hydrosphere.mist.{Messages, MistConfig}
import scala.concurrent.duration.DurationInt
import Messages.{CreateContext, StopAllContexts, RemoveContext, WorkerDidStart}
import io.hydrosphere.mist.jobs.JobConfiguration
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import sys.process._

/** Manages context repository */
private[mist] class WorkerManager extends Actor {

  private val cluster = Cluster(context.system)

  private val workers = WorkerCollection()

  def startNewWorkerWithName(name: String): Unit = {
    if (!workers.contains(name)) {
      new Thread {
        override def run() = {
          val configFile = System.getProperty("config.file")
          val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
          s"${sys.env("MIST_HOME")}/mist.sh worker --namespace $name --config $configFile --jar $jarPath" !
        }
      }.start()
    }
  }

  def removeWorkerByName(name: String): Unit = {
    if (workers.contains(name)) {
      val address = workers(name).address
      workers -= WorkerLink(name, address)
      cluster.leave(AddressFromURIString(address))
    }
  }

  override def receive: Receive = {
    case CreateContext(name) =>
      startNewWorkerWithName(name)

    // surprise: stops all contexts
    case StopAllContexts =>
      workers.foreach {
        case WorkerLink(name, address) =>
          removeWorkerByName(name)
      }

    // removes context
    case RemoveContext(name) =>
      removeWorkerByName(name)

    case WorkerDidStart(name, address) =>
      println(s"Worker `$name` did start on $address")
      workers += WorkerLink(name, address)

    case jobRequest: JobConfiguration =>
      val originalSender = sender
      startNewWorkerWithName(jobRequest.name)

      workers.registerCallbackForName(jobRequest.name, {
        case WorkerLink(name, address) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")

          val future = remoteActor.ask(jobRequest)(timeout = 248.days)
          future.onSuccess {
            case response: Any =>
              if (MistConfig.Contexts.isDisposable(name)) {
                removeWorkerByName(name)
              }
              originalSender ! response
          }
      })

  }

}
