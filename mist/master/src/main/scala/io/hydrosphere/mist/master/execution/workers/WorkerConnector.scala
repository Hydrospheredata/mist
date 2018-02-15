package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.{CompleteAndShutdown, ForceShutdown}
import io.hydrosphere.mist.master.execution.SpawnSettings
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.akka.{ActorRegHub, WhenTerminated}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

trait WorkerConnector {

  def askConnection(): Future[WorkerConnection]

  def warmUp(): Unit

  def shutdown(force: Boolean): Future[Unit]

  def whenTerminated(): Future[Unit]

}

object WorkerConnector {

  sealed trait Event
  object Event {
    final case class AskConnection(resolve: Promise[WorkerConnection]) extends Event
    case object WarnUp extends Event
    case object ConnTerminated extends Event
  }

  class ActorBasedWorkerConnector(
    underlying: ActorRef,
    termination: Future[Unit]
  ) extends WorkerConnector {

    override def askConnection(): Future[WorkerConnection] = {
      val promise = Promise[WorkerConnection]
      underlying ! WorkerConnector.Event.AskConnection(promise)
      promise.future
    }

    override def shutdown(force: Boolean): Future[Unit] = {
      val msg = if (force) ForceShutdown else CompleteAndShutdown
      underlying ! msg
      whenTerminated()
    }

    override def whenTerminated(): Future[Unit] = termination

    override def warmUp(): Unit = underlying ! WorkerConnector.Event.WarnUp

  }

  def startWorker(
    spawn: SpawnSettings,
    regHub: ActorRegHub,
    af: ActorRefFactory)(id: String, ctx: ContextConfig): Future[WorkerConnection] = {

    import spawn._

    runner.runWorker(id, ctx)
    val initInfo = toWorkerInitInfo(ctx)
    for {
      ref <- regHub.waitRef(id, timeout)
      connection <- WorkerBridge.connect(id, initInfo, readyTimeout, ref)(af)
    } yield connection
}

  def sharedConnector(
    regHub: ActorRegHub,
    spawn: SpawnSettings,
    af: ActorRefFactory)
    (id: String, ctx: ContextConfig): WorkerConnector = {
    val props = SharedConnector.props(id, ctx, startWorker(spawn, regHub, af))
    val actor = af.actorOf(props)
    new ActorBasedWorkerConnector(actor, WhenTerminated(actor)(af))
  }

  def exclusiveConnector(
    regHub: ActorRegHub,
    spawn: SpawnSettings,
    af: ActorRefFactory)
    (id: String, ctx: ContextConfig): WorkerConnector = {
    val props = ExclusiveConnector.props(id, ctx, startWorker(spawn, regHub, af))
    val actor = af.actorOf(props)
    new ActorBasedWorkerConnector(actor, WhenTerminated(actor)(af))
  }

  def default(
    regHub: ActorRegHub,
    spawn: SpawnSettings,
    af: ActorRefFactory)
    (id: String, ctx: ContextConfig): WorkerConnector = {
    val func = ctx.workerMode match {
      case RunMode.Shared => sharedConnector _
      case RunMode.ExclusiveContext => exclusiveConnector _
    }
    func(regHub, spawn, af)(id, ctx)
  }

}

