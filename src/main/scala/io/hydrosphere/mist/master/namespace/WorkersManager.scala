package io.hydrosphere.mist.master.namespace

import java.io.File

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster._
import io.hydrosphere.mist.Messages.StopWorker
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.namespace.ClusterWorker.WorkerRegistration
import io.hydrosphere.mist.master.namespace.WorkersManager.{GetWorkers, WorkerCommand, WorkerDown, WorkerUp}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Worker state: Down, Initialized, Started
  */
sealed trait WorkerState {
  val frontend: ActorRef
}

/**
  * Worker is down
  */
case class Down(frontend: ActorRef) extends WorkerState

/**
  * Worker is starting, but not already registered
  */
case class Initializing(frontend: ActorRef) extends WorkerState

/**
  * Workers is started and registered
  */
case class Started(
  frontend: ActorRef,
  address: Address,
  backend: ActorSelection
) extends WorkerState

/**
  * Manager for workers lifecycle
  * @param frontendFactory - function for obtaining frontendActors (for testing purposes)
  * @param workerRunner - interface for spawning workers processes
  */
class WorkersManager(
  frontendFactory: String => ActorRef,
  workerRunner: WorkerRunner
)extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  val workerStates = mutable.Map[String, WorkerState]()

  override def receive: Receive = {
    case WorkerCommand(name, entry) =>
      val state = getWorkerState(name)
      state.frontend forward entry

    case GetWorkers =>
      val aliveWorkers = workerStates.collect({case (name, x:Started) => name})
      sender() ! aliveWorkers

    case StopWorker(name) =>
      workerStates.get(name).foreach({
        case s: Started =>
          cluster.leave(s.address)
          setWorkerDown(name)
        case _ =>
      })

    case r @ WorkerRegistration(name, address) =>
      workerStates.get(name) match {
        case Some(s) =>
          val backend = cluster.system.actorSelection(RootActorPath(address) / "user" / s"worker-$name")
          workerStates += name -> Started(s.frontend, address, backend)
          s.frontend ! WorkerUp(backend)

        case None =>
          log.info(s"Received unexpected registration $r")
      }

    case UnreachableMember(m) =>
      workerNameFromMember(m).foreach(name => {
        setWorkerDown(name)
      })

    case MemberExited(m) =>
      workerNameFromMember(m).foreach(name => {
        setWorkerDown(name)
      })
  }

  private def workerNameFromMember(m: Member): Option[String] = {
    m.getRoles
      .find(_.startsWith("worker-"))
      .map(_.replace("worker_", ""))
  }

  private def setWorkerDown(name: String): Unit = {
    workerStates.get(name) match {
      case Some(s) =>
        s match {
          case d: Down => // ignore event - it's already down
          case _ =>
            workerStates += name -> Down(s.frontend)
            s.frontend ! WorkerDown
            log.info(s"Worker for $name is marked down")
        }

      case None =>
        log.info(s"Received unexpected unreachable worker $name")
    }
  }

  private def getWorkerState(name: String): WorkerState = {
    workerStates.getOrElse(name, defaultWorkerState(name)) match {
      case d: Down =>
        runWorker(name)
        val state = Initializing(d.frontend)
        workerStates += name -> state
        state
      case x => x
    }
  }

  private def runWorker(name: String): Unit = {
    val settings = WorkerSettings(
      name = name,
      runOptions = MistConfig.Contexts.runOptions(name),
      configFilePath = System.getProperty("config.file"),
      jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).toString
    )
    workerRunner.run(settings)
//    //TODO ???
//    val sparkHome = System.getenv("SPARK_HOME").ensuring(_.nonEmpty, "SPARK_HOME is not defined!")
//    new LocalWorkerRunner(sparkHome).run(settings)
  }

  private def defaultWorkerState(name: String): WorkerState = {
    val frontend = context.actorOf(NamespaceJobExecutor.props(name, 10))
    Down(frontend)
  }

  private def isWorker(m: Member): Boolean =
    m.getRoles.exists(_.startsWith("worker-"))

  override def preStart(): Unit = {
    cluster.subscribe(self,
      InitialStateAsEvents,
      classOf[MemberEvent],
      classOf[UnreachableMember])

    log.debug(self.toString())
  }
}

object WorkersManager {

  case class WorkerCommand(name: String, message: Any)

  case object GetWorkers
  case class StopWorker(name: String)

  case class WorkerUp(ref: ActorSelection)
  case object WorkerDown
}
