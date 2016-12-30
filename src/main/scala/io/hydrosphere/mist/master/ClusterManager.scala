package io.hydrosphere.mist.master

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, AddressFromURIString, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.worker.{JobDescriptionSerializable, LocalNode, WorkerDescription}
import io.hydrosphere.mist.{Constants, MistConfig, Worker}
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success}

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

  override def receive: Receive = {

    case ListRouters =>
      val config = ConfigFactory.parseFile(new File(MistConfig.HTTP.routerConfigPath))
      val javaMap: java.util.Map[String, Object] = config.root().unwrapped()

      import scala.collection.JavaConversions._
      val scalaMap: Map[String, Any] = javaMap.toMap.map { case (k, v) => (k, v.toString)}

      sender ! scalaMap //TODO

    case StopJob(message) => {
      val originalSender = sender
      val future: Future[List[String]] = Future {
        val stopResponse = ArrayBuffer.empty[String]
        if (message.contains(Constants.CLI.stopJobMsg)) {
          workers.foreach {
            case WorkerLink(name, address) => {
              val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
              val futureListJobs = remoteActor.ask(new StopJob(message))(timeout = Constants.CLI.timeoutDuration)
              val result = Await.result(futureListJobs, Constants.CLI.timeoutDuration).asInstanceOf[List[String]]
              result.map(jobStopResponse => stopResponse += jobStopResponse)
            }
          }
        }
        stopResponse.toList
      }

      future onComplete {
        case Success(result: List[String]) => originalSender ! result
        case Failure(error: Throwable) => originalSender ! error
      }
    }

    case ListWorkers =>
      val workerDescriptions = ArrayBuffer.empty[WorkerDescription]
      workers
        .foreach {
          case WorkerLink(name, address) => {
            workerDescriptions += new WorkerDescription(name, address)
          }
        }
      sender ! workerDescriptions.toList

    case ListJobs =>
      val originalSender = sender

      val future: Future[List[JobDescriptionSerializable]] = Future {
        val jobDescriptionsSerializable = ArrayBuffer.empty[JobDescriptionSerializable]
        workers
          .foreach {
            case WorkerLink(name, address) => {
              val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
              val futureListJobs = remoteActor.ask(ListJobs)(timeout = Constants.CLI.timeoutDuration)
              val result = Await.result(futureListJobs, Constants.CLI.timeoutDuration).asInstanceOf[List[JobDescriptionSerializable]]
              result.map(job => jobDescriptionsSerializable += job)
            }
          }
        jobDescriptionsSerializable.toList
      }

      future onComplete {
        case Success(result: List[JobDescriptionSerializable]) => originalSender ! result
        case Failure(error: Throwable) => originalSender ! error
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
