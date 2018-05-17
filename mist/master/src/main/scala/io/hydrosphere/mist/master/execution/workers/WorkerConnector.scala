package io.hydrosphere.mist.master.execution.workers

import akka.actor.{ActorRef, ActorRefFactory}
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.akka.WhenTerminated

import scala.concurrent.{Future, Promise}

trait WorkerConnector {

  def askConnection(): Future[PerJobConnection]

  def warmUp(): Unit

  def shutdown(force: Boolean): Future[Unit]

  def whenTerminated(): Future[Unit]

}

object WorkerConnector {

  sealed trait Event
  object Event {
    final case class AskConnection(resolve: Promise[PerJobConnection]) extends Event
    final case class Released(conn: WorkerConnection) extends Event
    final case class Shutdown(force: Boolean) extends Event
    case object WarmUp extends Event
    final case class ConnTerminated(connId: String) extends Event
    case object GetStatus
  }

  class ActorBasedWorkerConnector(
    underlying: ActorRef,
    termination: Future[Unit]
  ) extends WorkerConnector {

    override def askConnection(): Future[PerJobConnection] = {
      val promise = Promise[PerJobConnection]
      underlying ! WorkerConnector.Event.AskConnection(promise)
      promise.future
    }

    override def shutdown(force: Boolean): Future[Unit] = {
      underlying ! Event.Shutdown(force)
      whenTerminated()
    }

    override def whenTerminated(): Future[Unit] = termination

    override def warmUp(): Unit = underlying ! WorkerConnector.Event.WarmUp


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

