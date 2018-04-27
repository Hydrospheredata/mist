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

/**
  * Direct connection to remote worker
  */
class WorkerBridge(
  id: String,
  initInfo: WorkerInitInfo,
  remote: ActorRef,
  ready: Promise[WorkerConnection],
  readyTimeout: FiniteDuration,
  stopAction: StopAction
) extends Actor with ActorLogging with Timers{

  import WorkerBridge._

  private val initTimerKey = s"worker-conn-$id"

  override def preStart(): Unit = {
    context watch remote
    remote ! initInfo
    timers.startSingleTimer(initTimerKey, InitTimeout, readyTimeout)
  }

  override def receive: Receive = connecting

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
      context become process(terminated)

    case WorkerStartFailed(wId, msg) if wId == id =>
      timers.cancel(initTimerKey)
      val expl = s"Worker $id instantiation failed:" + msg
      log.error(expl)
      ready.failure(new RuntimeException(expl))
      context stop self

    case InitTimeout =>
      val msg = s"Worker $id was terminated during initialization"
      log.warning(msg)
      ready.failure(new RuntimeException(msg))
      context stop self

    case Terminated(_) =>
      val msg = s"Worker $id was terminated during initialization"
      log.warning(msg)
      ready.failure(new RuntimeException(msg))
      context stop self
  }

  private def process(terminated: Promise[Unit]): Receive = {
    case Terminated(_) =>
      terminated.failure(new RuntimeException(s"Connection with worker $id was closed"))
      context stop self

    case Shutdown =>
      log.info(s"Received shutdown for worker conn $id")
      stopAction match {
        case StopAction.Remote => remote ! ForceShutdown
        case StopAction.CustomFn(f) => f(id)
      }
      terminated.success(())
      context unwatch remote
      context.setReceiveTimeout(1 minute)
      context become awaitStop

    case Goodbye =>
      log.info(s"Remote application was unexpectedly stopped")
      terminated.failure(new RuntimeException(s"Worker $id was stopped"))
      context stop self

    case other => remote forward other
  }

  // only for logging
  private def awaitStop: Receive = {
    case Goodbye =>
      log.info(s"Remote worker $id application was correctly stopped")
      context stop self

    case ReceiveTimeout =>
      log.error(s"Remote worker $id application wasn't correctly stopped")
      context stop self
  }

}

object WorkerBridge {

  case object InitTimeout
  case object Shutdown

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
