package io.hydrosphere.mist.master

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, AddressFromURIString, Props}
import akka.cluster.Cluster
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.cli.{JobDescription, WorkerDescription}
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.{Collections, ExternalJar, ExternalMethodArgument, Logger}
import io.hydrosphere.mist.worker.LocalNode
import io.hydrosphere.mist.{Constants, MistConfig, Worker}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success}

object ClusterManager {
  
  case class StartJob(jobDetails: JobDetails)

  def props(): Props = Props(classOf[ClusterManager])

  private val workers = WorkerCollection()
}

/** Manages context repository */
private[mist] class ClusterManager extends Actor with Logger {
// TODO: refactor
  private val cluster = Cluster(context.system)

  def startNewWorkerWithName(name: String): Unit = {
    if (!ClusterManager.workers.contains(name)) {
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
    if (ClusterManager.workers.contains(name)) {
      val address = ClusterManager.workers(name).address
      ClusterManager.workers -= WorkerLink(name, address)
      cluster.down(AddressFromURIString(address))
    }
  }

  override def receive: Receive = {

    case ListRouters(extended) =>
      val config = ConfigFactory.parseFile(new File(MistConfig.HTTP.routerConfigPath))
      val javaMap = config.root().unwrapped()
      
      val scalaMap: Map[String, Any] = Collections.asScalaRecursively(javaMap)
      
      if (extended) {
        sender ! scalaMap.map {
          case (key: String, value: Map[String, Any]) =>
            if (JobFile.fileType(value("path").asInstanceOf[String]) == JobFile.FileType.Python) {
              key -> (Map("isPython" -> true) ++ value)
            } else {
              val jobFile = JobFile(value("path").asInstanceOf[String])
              if (!jobFile.exists) {
                return null
              }
              
              val externalClass = ExternalJar(jobFile.file).getExternalClass(value("className").asInstanceOf[String])
              val inst = externalClass.getNewInstance
              
              def methodInfo(methodName: String): Map[String, Map[String, String]] = {
                try {
                  Map(methodName -> inst.getMethod(methodName).arguments.flatMap { arg: ExternalMethodArgument =>
                    Map(arg.name -> arg.tpe.toString)
                  }.toMap[String, String]
                  )
                } catch {
                  case _: Throwable => Map.empty[String, Map[String, String]]
                }
              } 
              
              val classInfo = Map(
                "isMLJob" -> externalClass.isMLJob,
                "isStreamingJob" -> externalClass.isStreamingJob,
                "isSqlJob" -> externalClass.isSqlJob,
                "isHiveJob" -> externalClass.isHiveJob
              )
              key -> (classInfo ++ value ++ methodInfo("execute") ++ methodInfo("train") ++ methodInfo("serve"))
            }
        }
      } else {
        sender ! scalaMap
      }
      

    case message: StopJob =>
      val originalSender = sender
      val future: Future[List[String]] = Future {
        ClusterManager.workers.map {
          case WorkerLink(name, address) =>
            val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
            val futureListJobs = remoteActor.ask(message)(timeout = Constants.CLI.timeoutDuration)
            Await.result(futureListJobs, Constants.CLI.timeoutDuration).asInstanceOf[List[String]]
        }.flatten
      }

      future onComplete {
        case Success(result: List[String]) => originalSender ! result
        case Failure(error: Throwable) => originalSender ! error
      }

    case ListWorkers() =>
      sender ! ClusterManager.workers.map {
        case WorkerLink(name, address) =>
          WorkerDescription(name, address)
      }

    case ListJobs() =>
      val originalSender = sender

      val future: Future[List[JobDetails]] = Future.sequence(ClusterManager.workers.map[Future[JobDetails]] {
        case WorkerLink(name, address) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
          remoteActor.ask(ListJobs)(timeout = Constants.CLI.timeoutDuration).asInstanceOf[Future[JobDetails]]
      })

      future onComplete {
        case Success(result: List[JobDetails]) => originalSender ! result
        case Failure(error: Throwable) => originalSender ! error
      }

    case CreateContext(name) =>
      startNewWorkerWithName(name)

    case _: StopAllMessage =>
      ClusterManager.workers.foreach {
        case WorkerLink(name, _) =>
          removeWorkerByName(name)
      }
      sender ! Constants.CLI.stopAllWorkers

    case message: RemovingMessage =>
      removeWorkerByName(message.name)
      sender ! s"Worker ${message.name} is scheduled for shutdown."

    case WorkerDidStart(name, address) =>
      logger.info(s"Worker `$name` did start on $address")
      ClusterManager.workers += WorkerLink(name, address)

    case jobDetails: JobDetails =>
      context.actorOf(JobDistributor.props()) ! jobDetails

    case ClusterManager.StartJob(jobDetails) if jobDetails.configuration.isInstanceOf[ServingJobConfiguration] =>
      val originalSender = sender
      val localNodeActor = context.actorOf(Props(classOf[LocalNode]))
      val future = localNodeActor.ask(jobDetails)(timeout = FiniteDuration(MistConfig.Contexts.timeout(jobDetails.configuration.namespace).toNanos, TimeUnit.NANOSECONDS))
      future onSuccess {
        case response => originalSender ! response
      }

    case ClusterManager.StartJob(jobDetails) =>
      startNewWorkerWithName(jobDetails.configuration.namespace)

      ClusterManager.workers.registerCallbackForName(jobDetails.configuration.namespace, {
        case WorkerLink(name, address) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
          remoteActor ! jobDetails
      })
      context become jobUpdates(sender)
      
  def jobUpdates(originalSender: ActorRef): Receive = {
    case jobDetails: JobDetails =>
      originalSender ! jobDetails
  }
    
//    case AddJobToRecovery(jobId, jobConfiguration) =>
//      if (MistConfig.Recovery.recoveryOn) {
//        lazy val configurationRepository: JobRepository = MistConfig.Recovery.recoveryTypeDb match {
//          case "MapDb" => InMapDbJobConfigurationRepository
//          case _ => InMemoryJobConfigurationRepository
//
//        }
//        configurationRepository.add(jobId, jobConfiguration)
//        val recoveryActor = context.system.actorSelection(cluster.selfAddress + "/user/RecoveryActor")
//        recoveryActor ! JobStarted
//      }
//
//    case RemoveJobFromRecovery(jobId) =>
//      if (MistConfig.Recovery.recoveryOn) {
//        lazy val configurationRepository: JobRepository = MistConfig.Recovery.recoveryTypeDb match {
//          case "MapDb" => InMapDbJobConfigurationRepository
//          case _ => InMemoryJobConfigurationRepository
//
//        }
//        configurationRepository.remove(jobId)
//        val recoveryActor = context.system.actorSelection(cluster.selfAddress + "/user/RecoveryActor")
//        recoveryActor ! JobCompleted
//      }
//
  }

}
