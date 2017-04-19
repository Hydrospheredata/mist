package io.hydrosphere.mist.master

import java.io.File

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.WorkersManager.WorkerResolved

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

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
  backend: ActorRef
) extends WorkerState

/**
  * Manager for workers lifecycle
  *
  * @param workerRunner - interface for spawning workers processes
  */
class WorkersManager(
  statusService: ActorRef,
  workerRunner: WorkerRunner
)extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  val workerStates = mutable.Map[String, WorkerState]()

  override def receive: Receive = {
    case WorkerCommand(name, entry) =>
      val state = getOrRunWorker(name)
      state.frontend forward entry

    case GetWorkers =>
      sender() ! aliveWorkers

    case GetActiveJobs =>
      implicit val timeout = Timeout(1.second)
      val allRequests = workerStates.values
        .map(state => (state.frontend ? GetActiveJobs).mapTo[List[JobExecutionStatus]])
      val future = Future.sequence(allRequests).map(_.flatten.toList)
      future pipeTo sender()

    case StopWorker(name) =>
      workerStates.get(name).foreach({
        case s: Started =>
          cluster.leave(s.address)
          setWorkerDown(name)
          sender() ! akka.actor.Status.Success(())
        case _ =>
      })

    case StopAllWorkers =>
      workerStates.foreach({case (name, state) =>
        state match {
          case s: Started =>
            cluster.leave(s.address)
            setWorkerDown(name)
          case _ =>
        }
      })
      sender() ! akka.actor.Status.Success(())

    case r @ WorkerRegistration(name, address) =>
      val selection = cluster.system.actorSelection(RootActorPath(address) / "user" / s"worker-$name")
      selection.resolveOne(5.second).onComplete({
        case Success(ref) => self ! WorkerResolved(name, address, ref)
        case Failure(e) =>
          log.error(e, s"Worker reference resolution failed for $name")
      })

    case r @ WorkerResolved(name, address, ref) =>
      val s = getOrRunWorker(name)
      workerStates += name -> Started(s.frontend, address, ref)
      s.frontend ! WorkerUp(ref)
      log.info(s"Worker with $name is registered on $address")

    case UnreachableMember(m) =>
      aliveWorkers.find(_.address == m.address.toString)
        .foreach(worker => setWorkerDown(worker.name))

    case MemberExited(m) =>
      workerNameFromMember(m).foreach(name => {
        setWorkerDown(name)
      })

    case CreateContext(name) =>
      getOrRunWorker(name)
  }

  private def aliveWorkers: List[WorkerLink] = {
    workerStates.collect({
      case (name, x:Started) => WorkerLink(name, x.address.toString)
    }).toList
  }

  private def workerNameFromMember(m: Member): Option[String] = {
    m.getRoles
      .find(_.startsWith("worker-"))
      .map(_.replace("worker-", ""))
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
            workerRunner.onStop(name)
        }

      case None =>
        log.info(s"Received unexpected unreachable worker $name")
    }
  }

  private def getOrRunWorker(name: String): WorkerState = {
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
  }

  private def defaultWorkerState(name: String): WorkerState = {
    val frontend = context.actorOf(FrontendJobExecutor.props(name, 10, statusService))
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

  def props(
    statusService: ActorRef,
    workerRunner: WorkerRunner): Props = {

    Props(classOf[WorkersManager], statusService, workerRunner)
  }

  case class WorkerResolved(name: String, address: Address, ref: ActorRef)
}
