package io.hydrosphere.mist.master

import java.io.File
import java.util.UUID

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.master.WorkersManager.WorkerResolved
import io.hydrosphere.mist.master.models.RunMode

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Worker state: Down, Initialized, Started
  */
sealed trait WorkerState {
  val frontend: ActorRef
  def forward(msg: Any)(implicit context: ActorContext): Unit = frontend forward msg
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
) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  val workerStates = mutable.Map[String, WorkerState]()

  override def receive: Receive = {
    case RunJobCommand(contextId, mode, jobRequest) =>
      workerStates.get(contextId) match {
        case Some(s) => s.forward(jobRequest)
        case None =>
          val state = startWorker(contextId, mode)
          state.forward(jobRequest)
      }
    case c @ CancelJobCommand(name, req) =>
      workerStates.get(name) match {
        case Some(s) => s.forward(req)
        case None => log.warning("Handled message {} for unknown worker", c)
      }

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
      workerStates.get(name) match {
        case Some(state) =>
          val next = Started(state.frontend, address, ref)
          workerStates += name ->next
          state.frontend ! WorkerUp(ref)
          log.info(s"Worker with $name is registered on $address")

        case None =>
          log.warning("Received memberResolve from unknown worker {}", name)
      }

    case UnreachableMember(m) =>
      aliveWorkers.find(_.address == m.address.toString)
        .foreach(worker => setWorkerDown(worker.name))

    case MemberExited(m) =>
      workerNameFromMember(m).foreach(name => {
        setWorkerDown(name)
      })

    case CreateContext(contextId) =>
      startWorker(contextId, RunMode.Default)
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

  private def startWorker(contextId: String, mode: RunMode): WorkerState = {
    val workerId = mode match {
      case RunMode.Default => contextId
      case RunMode.ExclusiveContext(id) =>
        val uuid = UUID.randomUUID().toString
        val postfix = id.map(s => s"$s-$uuid").getOrElse(uuid)
        s"$contextId-$postfix"
    }

    runWorker(workerId, contextId, mode)
    val defaultState = defaultWorkerState(workerId)
    val state = Initializing(defaultState.frontend)
    workerStates += workerId -> state

    state
  }

  private def runWorker(
    name: String,
    context: String,
    mode: RunMode
  ): Unit = {
    val settings = WorkerSettings(
      name = name,
      context = context,
      runOptions = MistConfig.Contexts.runOptions(name),
      configFilePath = System.getProperty("config.file"),
      jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath).toString,
      mode = mode
    )
    workerRunner.run(settings)
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
