package io.hydrosphere.mist.master

import akka.actor.{Actor, AddressFromURIString}
import akka.pattern.ask
import akka.cluster.Cluster
import io.hydrosphere.mist.{Master, Messages, MistConfig, Worker}

import scala.concurrent.duration.DurationInt
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._

import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
// scalastyle:off
import sys.process._
// scalastyle:on

/** Manages context repository */
private[mist] class WorkerManager extends Actor {

  private val cluster = Cluster(context.system)

  private val workers = WorkerCollection()

  def startNewWorkerWithName(name: String): Unit = {
    if (!workers.contains(name)) {
      new Thread {
        override def run() = {
          val value = sys.env.get("MIST_HOME")
          //value.getOrElse(".") + "/mist.sh worker --namespace " + name !
          Worker.main(Array(name))
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

    case AddJobToRecovery(jobId, jobConfiguration) =>
      if (MistConfig.Recovery.recoveryOn == true) {
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository

        }
        configurationRepository.add(jobId, jobConfiguration)
        Master.recoveryActor ! JobStarted
      }

    case RemoveJobFromRecovery(jobId) =>
      if (MistConfig.Recovery.recoveryOn == true) {
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository

        }
        configurationRepository.remove(jobId)
        Master.recoveryActor ! JobCompleted
      }

  }

}
