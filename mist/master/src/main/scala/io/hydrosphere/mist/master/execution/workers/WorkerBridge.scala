package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated, Timers}
import io.hydrosphere.mist.core.CommonData.{WorkerInitInfo, WorkerReady}
import io.hydrosphere.mist.master.WorkerLink
import io.hydrosphere.mist.utils.akka.WhenTerminated

import scala.concurrent.Promise
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
    timers.startSingleTimer(initTimerKey, Event.InitTimeout, readyTimeout)
  }

  override def receive: Receive = initialiazing

  private def initialiazing: Receive = {
    case WorkerReady(wId, sparkUi) if wId == id =>
      timers.cancel(initTimerKey)
      val terminated = Promise[Unit]
      val connection = WorkerConnection(
        id = id,
        ref = self,
        data = WorkerLink(id, sender().path.toString, sparkUi),
        whenTerminated = terminated.future
      )
      ready.success(connection)
      context become process(terminated)

    case Event.InitTimeout =>
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

    case other => remote forward other
  }

}

object WorkerBridge {

  sealed trait Event
  object Event {
    case object InitTimeout
  }

  def props(
    id: String,
    initInfo: WorkerInitInfo,
    remote: ActorRef,
    ready: Promise[WorkerConnection],
    readyTimeout: FiniteDuration
  ): Props = Props(classOf[WorkerBridge], id, initInfo, remote, ready, readyTimeout)

}
