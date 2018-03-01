package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated, Timers}
import io.hydrosphere.mist.core.CommonData.{ForceShutdown, WorkerInitInfo, WorkerReady, WorkerStartFailed}
import io.hydrosphere.mist.master.execution.WorkerLink

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration

/**
  * Direct connection to remote worker
  */
class WorkerBridge(
  id: String,
  initInfo: WorkerInitInfo,
  remote: ActorRef,
  ready: Promise[WorkerConnection],
  readyTimeout: FiniteDuration
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
      terminated.success(())
      context stop self

    case ForceShutdown =>
      log.info("Received force shutdown")
      remote ! ForceShutdown
      terminated.success(())
      context stop self

    case other => remote forward other
  }

}

object WorkerBridge {

  case object InitTimeout

  def props(
    id: String,
    initInfo: WorkerInitInfo,
    readyTimeout: FiniteDuration,
    ready: Promise[WorkerConnection],
    remote: ActorRef
  ): Props = Props(classOf[WorkerBridge], id, initInfo, remote, ready, readyTimeout)

  def connect(
    id: String,
    initInfo: WorkerInitInfo,
    readyTimeout: FiniteDuration,
    remote: ActorRef
  )(implicit af: ActorRefFactory): Future[WorkerConnection] = {
    val ready = Promise[WorkerConnection]
    af.actorOf(props(id, initInfo, readyTimeout, ready, remote))
    ready.future
  }

}
