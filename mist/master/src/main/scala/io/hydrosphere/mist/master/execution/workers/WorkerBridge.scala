package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, ReceiveTimeout, Terminated, Timers}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.master.execution.WorkerLink

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

sealed trait StopAction
object StopAction {
  case object Remote extends StopAction
  final case class CustomFn(f: String => Unit) extends StopAction
}

class WorkerBridge(
  id: String,
  initInfo: WorkerInitInfo,
  remote: ActorRef,
  ready: Promise[WorkerConnection],
  readyTimeout: FiniteDuration,
  stopAction: StopAction
) extends Actor with ActorLogging with Timers {

  import WorkerBridge._

  override def receive: Receive = connecting

  private val initTimerKey = s"worker-conn-$id"

  override def preStart(): Unit = {
    context watch remote
    remote ! initInfo
    timers.startSingleTimer(initTimerKey, Event.InitTimeout, readyTimeout)
  }

  // call custom stop action if we lose connection to worker node
  private def makeControllingShot(): Unit = stopAction match {
    case StopAction.CustomFn(f) => f(id)
    case StopAction.Remote =>
  }

  private def connecting: Receive = {
    case WorkerReady(wId, sparkUi) if wId == id =>
      timers.cancel(initTimerKey)
      val terminated = Promise[Unit]
      val connection = WorkerConnection(
        id = id,
        ref = self,
        data = WorkerLink(id, sender().path.toString, sparkUi, initInfo),
        whenTerminated = terminated.future
      )
      ready.success(connection)
      context become awaitRequest(terminated)

    case WorkerStartFailed(wId, msg) if wId == id =>
      timers.cancel(initTimerKey)
      val expl = s"Worker $id instantiation failed:" + msg
      log.error(expl)
      ready.failure(new RuntimeException(expl))
      context stop self

    case Event.InitTimeout =>
      val msg = s"Worker $id was terminated during initialization"
      log.warning(msg)
      ready.failure(new RuntimeException(msg))
      context stop self

    case Terminated(_) =>
      ready.failure(new RuntimeException(s"Connection with worker $id was closed"))
      makeControllingShot()
      context stop self
  }

  private def startShuttingDown(): Unit = {
    remote ! ShutdownWorker
    context unwatch remote
    context.setReceiveTimeout(1 minute)
    context become stopping()
  }

  private def awaitRequest(term: Promise[Unit]): Receive = {
    case req: RunJobRequest =>
      remote ! req
      context become execution(term, sender(), false)

    case _ @ (_: Event.ShutdownCommand | _: RequestTermination.type) =>
      term.success(())
      startShuttingDown()

    case Terminated(_) =>
      term.failure(new RuntimeException(s"Connection with worker $id was closed"))
      makeControllingShot()
      context stop self
  }

  private def execution(
    term: Promise[Unit],
    orig: ActorRef,
    completeAndShutdown: Boolean
  ): Receive = {
    case req: RunJobRequest => sender() ! WorkerIsBusy
    case x: RunJobResponse => orig ! x
    case x: JobIsCancelled => orig ! x
    case cancel: CancelJobRequest => remote ! cancel
    case x: JobResponse =>
      orig ! x
      if (completeAndShutdown) {
        term.success(())
        startShuttingDown()
      } else {
        context become awaitRequest(term)
      }

    case Event.CompleteAndShutdown => context become execution(term, orig, true)
    case Event.ForceShutdown | RequestTermination =>
      term.failure(new RuntimeException(s"Worker $id was stopped forcibly"))
      startShuttingDown()

    case Terminated(_) =>
      term.failure(new RuntimeException(s"Connection with worker $id was closed"))
      makeControllingShot()
      context stop self
  }

  private def stopping(): Receive = {
    case RequestTermination =>
      stopAction match {
        case StopAction.Remote => remote ! ShutdownWorkerApp
        case StopAction.CustomFn(f) => f(id)
      }
      context setReceiveTimeout(1 minute)
      context become disconnecting()

    case ReceiveTimeout =>
      log.error(s"Remote worker $id application wasn't correctly stopped")
      makeControllingShot()
      context stop self
  }

  private def disconnecting(): Receive = {
    case Goodbye =>
      log.info(s"Remote worker $id application was correctly stopped")
      context stop self

    case ReceiveTimeout =>
      log.error(s"Remote worker $id application wasn't correctly stopped")
      context stop self
  }
}

object WorkerBridge {

  sealed trait Event
  object Event {
    case object InitTimeout
    sealed trait ShutdownCommand extends Event
    case object CompleteAndShutdown extends ShutdownCommand
    case object ForceShutdown extends ShutdownCommand
  }

  def props(
    id: String,
    initInfo: WorkerInitInfo,
    readyTimeout: FiniteDuration,
    ready: Promise[WorkerConnection],
    remote: ActorRef,
    stopAction: StopAction
  ): Props = Props(classOf[WorkerBridge], id, initInfo, remote, ready, readyTimeout, stopAction)

  def connect(
    id: String,
    initInfo: WorkerInitInfo,
    readyTimeout: FiniteDuration,
    remote: ActorRef,
    stopAction: StopAction
  )(implicit af: ActorRefFactory): Future[WorkerConnection] = {
    val ready = Promise[WorkerConnection]
    af.actorOf(props(id, initInfo, readyTimeout, ready, remote, stopAction))
    ready.future
  }
}
