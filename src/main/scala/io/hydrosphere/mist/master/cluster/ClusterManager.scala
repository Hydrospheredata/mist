package io.hydrosphere.mist.master.cluster

import java.io.File

import akka.actor.{Actor, AddressFromURIString, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberRemoved, UnreachableMember}
import akka.pattern.ask
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs._
import io.hydrosphere.mist.master.{JobDispatcher, WorkerCollection, WorkerLink}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.{Constants, MistConfig, Worker}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success}

private[mist] object ClusterManager {

  case class CreateContext(namespace: String)
  case class GetWorker(namespace: String)
  case class GetWorkers()
  
  def props(): Props = Props(classOf[ClusterManager])
  
}

/** Manages context repository */
private[mist] class ClusterManager extends Actor with Logger {
  val workers = WorkerCollection()

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
  
  private val cluster = Cluster(context.system)
  
  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    logger.debug(self.toString())
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

  def removeWorkerByAddress(address: String): Unit = {
    val uid = workers.getUIDByAddress(address)
    removeWorkerByUID(uid)
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

  override def receive: Receive = {

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

    case ClusterManager.CreateContext(name) =>
      startNewWorkerWithName(name)

    case _: StopAllMessage =>
      workers.foreach {
        case WorkerLink(uid, _, _, _) =>
          removeWorkerByUID(uid)
      }
      sender ! Constants.CLI.stopAllWorkers

    case message: RemovingMessage =>
      removeWorkerByName(message.contextIdentifier)
      removeWorkerByUID(message.contextIdentifier)
      sender ! s"Worker ${message.contextIdentifier} is scheduled for shutdown."

    case WorkerDidStart(uid, name, address) =>
      logger.info(s"Worker `$name` did start on $address")
      workers += WorkerLink(uid, name, address, blackSpot = false)

    case ClusterManager.GetWorker(namespace) =>
      startNewWorkerWithName(namespace)
      val originalSender = sender()
      workers.registerCallbackForName(namespace, {
        link: WorkerLink =>
          originalSender ! link
      })

    case ClusterManager.GetWorkers() =>
      logger.debug(s"GET WORKERS: ${workers.map(l => l).toString()}")
      sender() ! workers.map(l => l)

    case jobDetails: JobDetails =>
      context.actorOf(JobDispatcher.props()) ! jobDetails

    case MemberRemoved(member, _) =>
      removeWorkerByAddress(member.address.toString)

  }

}
