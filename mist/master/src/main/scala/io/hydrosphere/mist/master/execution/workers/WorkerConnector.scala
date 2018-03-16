package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.core.CommonData.{CompleteAndShutdown, ForceShutdown}
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.akka.WhenTerminated

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
    case class ConnTerminated(connId: String) extends Event
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

  def actorBased(
    id: String,
    ctx: ContextConfig,
    runner: WorkerRunner,
    af: ActorRefFactory
  ): WorkerConnector = {
    val props = ctx.workerMode match {
      case RunMode.Shared => SharedConnector.props(id, ctx, runner)
      case RunMode.ExclusiveContext => ExclusiveConnector.props(id, ctx, runner)
    }
    val actor = af.actorOf(props)
    new ActorBasedWorkerConnector(actor, WhenTerminated(actor)(af))
  }

}

