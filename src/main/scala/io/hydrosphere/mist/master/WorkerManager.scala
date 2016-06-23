package io.hydrosphere.mist.master

import akka.actor.{AddressFromURIString, Actor}
import akka.cluster.ClusterEvent._
import akka.pattern.ask
import akka.cluster._
import io.hydrosphere.mist.{Messages, MistConfig}
import scala.concurrent.duration._
import Messages._
import io.hydrosphere.mist.jobs.JobConfiguration
import sys.process._
import scala.concurrent.ExecutionContext.Implicits.global

/** Manages context repository */
private[mist] class WorkerManager extends Actor {

  private val cluster = Cluster(context.system)

  private val workers = WorkerCollection()

  def startNewWorkerWithName(name: String) = {
    if (!workers.contains(name)) {
      new Thread {
        override def run() = {
          sys.env("MIST_HOME") + "/mist.sh worker --namespace " + name !
        }
      }.start()
    }
  }

  def removeWorkerByName(name: String) = {
    if (workers.contains(name)) {
      val address = workers(name).address
      workers -= WorkerLink(name, address)
      cluster.leave(AddressFromURIString(address))
    }
  }

  override def preStart() {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop() {
    cluster.unsubscribe(self)
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
            case response =>
              if (MistConfig.Contexts.isDisposable(name)) {
                removeWorkerByName(name)
              }
              originalSender ! response
          }
      })

  }

}
