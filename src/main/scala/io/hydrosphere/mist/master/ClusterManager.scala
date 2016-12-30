package io.hydrosphere.mist.master

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorPath, AddressFromURIString, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MistConfig, Worker}
import io.hydrosphere.mist.worker.LocalNode

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.sys.process._

/** Manages context repository */
private[mist] class ClusterManager extends Actor with Logger {

  private val cluster = Cluster(context.system)

  private val workers = WorkerCollection()

  def startNewWorkerWithName(name: String): Unit = {
    if (!workers.contains(name)) {
      if (MistConfig.Settings.singleJVMMode) {
        Worker.main(Array(name))
      } else {
        new Thread {
          override def run(): Unit = {
            val runOptions = MistConfig.Contexts.runOptions(name)
            val configFile = System.getProperty("config.file")
            val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)

            MistConfig.Workers.runner match {
              case "local" =>
                print("STARTING WORKER: ")
                val cmd: Seq[String] = Seq(
                  s"${sys.env("MIST_HOME")}/bin/mist",
                  "start",
                  "worker",
                  "--runner", "local",
                  "--namespace", name,
                  "--config", configFile.toString,
                  "--jar", jarPath.toString,
                  "--run-options", runOptions)
                cmd !
              case "docker" =>
                val cmd: Seq[String] = Seq(
                  s"${sys.env("MIST_HOME")}/bin/mist",
                  "start",
                  "worker",
                  "--runner", "docker",
                  "--docker-host", MistConfig.Workers.dockerHost,
                  "--docker-port", MistConfig.Workers.dockerPort.toString,
                  "--namespace", name,
                  "--config", configFile.toString,
                  "--jar", jarPath.toString,
                  "--run-options", runOptions)
                cmd !
              case "manual" =>
                Process(
                  Seq("bash", "-c", MistConfig.Workers.cmd),
                  None,
                    "MIST_WORKER_NAMESPACE" -> name,
                    "MIST_WORKER_CONFIG" -> configFile.toString,
                    "MIST_WORKER_JAR_PATH" -> jarPath.toString,
                    "MIST_WORKER_RUN_OPTIONS" -> runOptions
                ).!
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
      cluster.down(AddressFromURIString(address))
    }
  }

  private var cliActorPath: ActorPath = _

  override def receive: Receive = {

    case StopMessage(message) => {
      if(message.contains(Constants.CLI.stopJobMsg)) {
        workers.foreach {
          case WorkerLink(name, address) => {
            try {
              val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
              remoteActor ! new StopMessage(message)
            }
          }
        }
      }
    }

    case StringMessage(message) =>
      if(message.contains(Constants.CLI.cliActorName)) {
        cliActorPath = sender.path
      }
      else if(message.contains(Constants.CLI.jobMsgMarker)) {
        val cliActor = cluster.system.actorSelection(cliActorPath)
        cliActor ! StringMessage(message.substring(Constants.CLI.jobMsgMarker.length).trim)
      }

    case ListMessage(message) =>
      cliActorPath = sender.path
      if(workers.isEmpty) {
        sender ! StringMessage(Constants.CLI.noWorkersMsg)
      }
      else if(message.contains(Constants.CLI.listJobsMsg) || message.contains(Constants.CLI.stopJobMsg)) {
        sender ! StringMessage("TIME\tNAMESPACE\tUID\tEXTERNAL ID\tROUTER")
        workers.foreach {
          case WorkerLink(name, address) =>
            val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
            remoteActor ! ListMessage(Constants.CLI.listJobsMsg + sender.path)
        }
      }
      else {
        sender ! StringMessage("NAMESPACE\tADDRESS")
        workers.foreach {
          case WorkerLink(name, address) =>
            sender ! StringMessage(s"$name\t$address")
        }
        sender ! StringMessage(Constants.CLI.jobMsgMarker + "it's all workers")
      }

    case CreateContext(name) =>
      startNewWorkerWithName(name)

    case StopAllContexts =>
      workers.foreach {
        case WorkerLink(name, _) =>
          removeWorkerByName(name)
      }

    case RemoveContext(name) =>
      removeWorkerByName(name)

    case WorkerDidStart(name, address) =>
      logger.info(s"Worker `$name` did start on $address")
      workers += WorkerLink(name, address)

    case jobRequest: ServingJobConfiguration =>
      val originalSender = sender
      val localNodeActor = context.system.actorOf(Props(classOf[LocalNode]))
      val future = localNodeActor.ask(jobRequest)(timeout = FiniteDuration(MistConfig.Contexts.timeout(jobRequest.namespace).toNanos, TimeUnit.NANOSECONDS))
      future onSuccess {
        case response => originalSender ! response
      }
      

    case jobRequest: FullJobConfiguration =>
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
