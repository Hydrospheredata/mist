package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, Props, Terminated}
import akka.pattern.pipe
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{Future, Promise}

class SharedConnector(
  id: String,
  ctx: ContextConfig,
  startConnection: (String, ContextConfig) => Future[WorkerConnection]
) extends Actor with ActorLogging {

  import context.dispatcher

  override def receive: Receive = noConnection

  private def noConnection: Receive = {
    case Event.AskConnection(req) =>
      startConnection(id, ctx) pipeTo self
      context become connecting(Seq(req))
  }

  private def connecting(requests: Seq[Promise[WorkerConnection]]): Receive = {
    case Event.AskConnection(req) =>
      context become connecting(req +: requests)

    case akka.actor.Status.Failure(e) =>
      log.error(e, s"Could not start worker connection for $id")
      context stop self

    case conn: WorkerConnection =>
      requests.foreach(_.success(conn))
      context watch conn.ref
      context become connected(conn)
  }

  private def connected(conn: WorkerConnection): Receive = {
    case Event.AskConnection(req) => req.success(conn)
    case Terminated(_) =>
      log.info(s"Worker connection was terminated for $id")
      context stop self
  }
}

object SharedConnector {

  def props(
    id: String,
    ctx: ContextConfig,
    startConnection: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(classOf[SharedConnector], id, ctx, startConnection)
}
