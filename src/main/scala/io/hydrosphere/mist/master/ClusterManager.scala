package io.hydrosphere.mist.master

import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, AddressFromURIString, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberRemoved, UnreachableMember}
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.utils.{Collections, ExternalJar, ExternalMethodArgument, Logger}
import io.hydrosphere.mist.worker.{JobDescriptionSerializable, LocalNode, WorkerDescription}
import io.hydrosphere.mist.{Constants, MistConfig, Worker}

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
    if (!workers.containsName(name)) {
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
  
  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def removeWorkerByName(name: String): Unit = {
    if (workers.containsName(name)) {
      val uid = workers.getUIDByName(name)
      removeWorkerByUID(uid)
    }
  }

  def removeWorkerByUID(uid: String): Unit = {
    val name = workers.getNameByUID(uid)
    if (workers.contains(name, uid)) {
      val address = workers(name, uid).address
      val blackSpot = workers(name, uid).blackSpot
      workers -= WorkerLink(uid, name, address, blackSpot)
      cluster.down(AddressFromURIString(address))
    }
  }

  def removeWorkerByAddress(address: String): Unit = {
    val uid = workers.getUIDByAddress(address)
    removeWorkerByUID(uid)
  }

  def restartWorkerWithName(name: String): Unit = {
    if (workers.containsName(name)) {
      val uid = workers.getUIDByName(name)
      val address = workers(name, uid).address
      val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
      if(MistConfig.Contexts.timeout(name).isFinite()) {
        remoteActor ! StopWhenAllDo
        workers.setBlackSpotByName(name)
        startNewWorkerWithName(name)
      } else {
        val uid = workers.getUIDByName(name)
        workers.setBlackSpotByName(name)
        startNewWorkerWithName(name)
        removeWorkerByUID(uid)
      }
    } else { startNewWorkerWithName(name) }
  }

  def runCmdStopByName(name: String): Unit = {
    if (MistConfig.Workers.cmdStop.nonEmpty && name.nonEmpty) {
      new Thread {
        override def run(): Unit = {
          MistConfig.Workers.runner match {
            case "manual" =>
              Process(
                Seq("bash", "-c", MistConfig.Workers.cmdStop.get),
                None,
                "MIST_WORKER_NAMESPACE" -> name
              ).!
          }
        }
      }.start()
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
        workers.map {
          case WorkerLink(_, name, address, _) =>
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
      sender ! workers.map {
        case WorkerLink(uid, name, address, blackSpot) =>
          WorkerDescription(uid, name, address, blackSpot)
      }

    case ListJobs() =>
      val originalSender = sender

      val future: Future[List[JobDescriptionSerializable]] = Future {
        val jobDescriptionsSerializable = ArrayBuffer.empty[JobDescriptionSerializable]
        workers
          .foreach {
            case WorkerLink(_, name, address, _) =>
              val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
              val futureListJobs = remoteActor.ask(ListJobs)(timeout = Constants.CLI.timeoutDuration)
              val result = Await.result(futureListJobs, Constants.CLI.timeoutDuration).asInstanceOf[List[JobDescriptionSerializable]]
              result.map(job => jobDescriptionsSerializable += job)
          }
        jobDescriptionsSerializable.toList
      }

      future onComplete {
        case Success(result: List[JobDescriptionSerializable]) => originalSender ! result
        case Failure(error: Throwable) => originalSender ! error
      }

    case CreateContext(name) =>
      startNewWorkerWithName(name)

    case _: StopAllMessage =>
      workers.foreach {
        case WorkerLink(uid, _, _, _) =>
          removeWorkerByUID(uid)
      }
      sender ! Constants.CLI.stopAllWorkers

    case message: RemovingMessage =>
      val name = {
        val workerName = workers.getNameByUID(message.contextIdentifier)
        if(workerName.nonEmpty) {
          workerName
        }
        else {
          message.contextIdentifier
        }
      }
      removeWorkerByName(message.contextIdentifier)
      removeWorkerByUID(message.contextIdentifier)
      sender ! s"Worker ${message.contextIdentifier} is scheduled for shutdown."
      runCmdStopByName(name)

    case WorkerDidStart(uid, name, address) =>
      logger.info(s"Worker `$name` did start on $address")
      workers += WorkerLink(uid, name, address, blackSpot = false)

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
        case WorkerLink(uid, name, address, false) =>
          val remoteActor = cluster.system.actorSelection(s"$address/user/$name")
          if(MistConfig.Contexts.timeout(jobRequest.namespace).isFinite()) {
            val future = remoteActor.ask(jobRequest)(timeout = FiniteDuration(MistConfig.Contexts.timeout(jobRequest.namespace).toNanos, TimeUnit.NANOSECONDS))
            future.onSuccess {
              case response: Any =>
                if (MistConfig.Contexts.isDisposable(name)) {
                  removeWorkerByUID(uid)
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

    case MemberRemoved(member, _) =>
      val uid = workers.getUIDByAddress(member.address.toString)
      val name = workers.getNameByUID(uid)
      removeWorkerByAddress(member.address.toString)

      runCmdStopByName(name)

  }

}
