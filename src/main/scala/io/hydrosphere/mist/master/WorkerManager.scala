package io.hydrosphere.mist.master

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, AddressFromURIString}
import akka.actor.{Actor, AddressFromURIString}
import akka.pattern.ask
import akka.cluster.Cluster
import akka.util.Timeout
import io.hydrosphere.mist.{Logger, MistConfig, Worker}

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._

import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import sys.process._

/** Manages context repository */
private[mist] class WorkerManager extends Actor with Logger{

  private val cluster = Cluster(context.system)

  private val workers = WorkerCollection()

  def startNewWorkerWithName(name: String): Unit = {
    if (!workers.contains(name)) {
      if (MistConfig.Settings.singleJVMMode) {
        Worker.main(Array(name))
      } else {
        new Thread {
          override def run() = {
            if (MistConfig.Workers.run == "local") {
              val configFile = System.getProperty("config.file")
              val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
              s"${sys.env("MIST_HOME")}/bin/mist start worker --runner local --namespace $name --config $configFile --jar $jarPath" !
            } else if (MistConfig.Workers.run == "docker") {
              val configFile = System.getProperty("config.file")
              val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
              s"${sys.env("MIST_HOME")}/bin/mist start worker --runner docker --host ${MistConfig.Workers.host} --port ${MistConfig.Workers.port}  --namespace $name --config $configFile --jar $jarPath" !
            }
          }
        }.start()
      }
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
      logger.info(s"Worker `$name` did start on $address")
      workers += WorkerLink(name, address)

    case jobRequest: FullJobConfiguration=>
      val originalSender = sender
      startNewWorkerWithName(jobRequest.namespace)

      workers.registerCallbackForName(jobRequest.namespace, {
        case WorkerLink(name, address) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
          if(MistConfig.Contexts.timeout(jobRequest.namespace).isFinite()) {
            val future = remoteActor.ask(jobRequest)(timeout = FiniteDuration(MistConfig.Contexts.timeout(jobRequest.namespace).toNanos, TimeUnit.NANOSECONDS))
            future.onSuccess {
              case response: Any =>
                if (MistConfig.Contexts.isDisposable(name)) {
                  removeWorkerByName(name)
                }
                originalSender ! response
            }
          }
          else {
            remoteActor ! jobRequest
          }
      })

    case AddJobToRecovery(jobId, jobConfiguration) =>
      if (MistConfig.Recovery.recoveryOn) {
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository

        }
        configurationRepository.add(jobId, jobConfiguration)
        val recoveryActor = context.system.actorSelection(cluster.selfAddress + "/user/RecoveryActor")
        recoveryActor ! JobStarted
      }

    case RemoveJobFromRecovery(jobId) =>
      if (MistConfig.Recovery.recoveryOn) {
        lazy val configurationRepository: ConfigurationRepository = MistConfig.Recovery.recoveryTypeDb match {
          case "MapDb" => InMapDbJobConfigurationRepository
          case _ => InMemoryJobConfigurationRepository

        }
        configurationRepository.remove(jobId)
        val recoveryActor = context.system.actorSelection(cluster.selfAddress + "/user/RecoveryActor")
        recoveryActor ! JobCompleted
      }

  }

}
