package io.hydrosphere.mist.master

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster._
import akka.pattern._
import akka.util.Timeout
import io.hydrosphere.mist.Messages.WorkerMessages._
import io.hydrosphere.mist.master.WorkersManager.WorkerResolved
import io.hydrosphere.mist.master.logging.JobsLogger
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}

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
  backend: ActorRef,
  sparkUi: Option[String]
) extends WorkerState

/**
  * Manager for workers lifecycle
  *
  * @param workerRunner - interface for spawning workers processes
  */
class WorkersManager(
  statusService: ActorRef,
  workerRunner: WorkerRunner,
  jobsLogger: JobsLogger,
  runnerInitTimeout: Duration,
  infoProvider: InfoProvider
) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  val workerStates = mutable.Map[String, WorkerState]()

  override def receive: Receive = {
    case r: RunJobCommand =>
      val workerId = r.computeWorkerId()
      workerStates.get(workerId) match {
        case Some(s) => s.forward(r.request)
        case None =>
          jobsLogger.info(r.request.id, s"Starting worker $workerId")
          val state = startWorker(workerId, r.context, r.mode)
          state.forward(r.request)
      }

    case c @ CancelJobCommand(name, req) =>
      workerStates.get(name) match {
        case Some(s) =>
          jobsLogger.info(req.id, "Cancel job manually")
          s.forward(req)
        case None => log.warning("Handled message {} for unknown worker", c)
      }

    case GetWorkers =>
      sender() ! aliveWorkers

    case WorkerInitInfoReq(ctxName) =>
      infoProvider.workerInitInfo(ctxName).pipeTo(sender())

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

    case r @ WorkerRegistration(name, address, sparkUi) =>
      log.info("Received worker registration - {}", r)
      val selection = cluster.system.actorSelection(RootActorPath(address) / "user" / s"worker-${name}")
      selection.resolveOne(5.second).onComplete({
        case Success(ref) => self ! WorkerResolved(name, address, ref, sparkUi)
        case Failure(e) =>
          log.error(e, s"Worker reference resolution failed for $name")
      })

    case r @ WorkerResolved(name, address, ref, sparkUi) =>
      log.info("Worker resolved - {}", r)
      workerStates.get(name) match {
        case Some(state) =>
          val next = Started(state.frontend, address, ref, sparkUi)
          workerStates += name ->next
          state.frontend ! WorkerUp(ref)
          log.info(s"Worker with $name is registered on $address")

        case None =>
          // this is possible when worker starting is too slow and we mark it as down (gg CheckInitWorkers)
          // and we already sent to frontend that jobs failed and removed default state of worker
          // so we here and we need to shutdown worker so actor does not leak
          cluster down address
          log.warning("Received memberResolve from unknown worker {}", name)
      }

    case CheckWorkerUp(id) =>
      checkWorkerUp(id)

    case UnreachableMember(m) =>
      aliveWorkers.find(_.address == m.address.toString)
        .foreach(worker => setWorkerDown(worker.name))

    case MemberExited(m) =>
      workerNameFromMember(m).foreach(name => {
        setWorkerDown(name)
      })

    case CreateContext(ctx) =>
      startWorker(ctx.name, ctx, RunMode.Shared)
  }

  private def checkWorkerUp(id: String): Unit = {
    workerStates.get(id).foreach({
      case state: Initializing =>
        val reason = s"Worker $id initialization timeout: not being responsive for $runnerInitTimeout"
        log.warning(reason)
        setWorkerDown(id)
        state.frontend ! FailRemainingJobs(reason)
      case _ =>
    })
  }

  private def aliveWorkers: List[WorkerLink] = {
    workerStates.collect({
      case (name, x:Started) => WorkerLink(name, x.address.toString, x.sparkUi)
    }).toList
  }

  private def workerNameFromMember(m: Member): Option[String] = {
    m.getRoles
      .find(_.startsWith("worker-"))
      .map(_.replace("worker-", ""))
  }

  private def startWorker(workerId: String, contextCfg: ContextConfig, mode: RunMode): WorkerState = {
    workerRunner.runWorker(workerId, contextCfg, mode)
    log.info("Trying to start worker {}, for context: {}", workerId, contextCfg.name)
    runnerInitTimeout match {
      case f: FiniteDuration =>
        context.system.scheduler.scheduleOnce(f, self, CheckWorkerUp(workerId))
      case _ =>
    }

    val defaultState = defaultWorkerState(workerId)
    val state = Initializing(defaultState.frontend)
    workerStates += workerId -> state

    state
  }

  private def setWorkerDown(name: String): Unit = {
    workerStates.get(name) match {
      case Some(s) =>
        s match {
          case d: Down => // ignore event - it's already down
          case _ =>
            workerStates -= name
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
    workerRunner: WorkerRunner,
    jobsLogger: JobsLogger,
    runnerInitTimeout: Duration,
    infoProvider: InfoProvider
  ): Props = {
    Props(classOf[WorkersManager], statusService, workerRunner, jobsLogger, runnerInitTimeout, infoProvider)
  }

  case class WorkerResolved(name: String, address: Address, ref: ActorRef, sparkUi: Option[String])
}
