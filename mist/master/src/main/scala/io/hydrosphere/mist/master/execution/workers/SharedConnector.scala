package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import akka.pattern.pipe
import io.hydrosphere.mist.core.CommonData.{ConnectionUnused, ShutdownCommand}
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

    case Event.WarnUp =>
      startConnection(id, ctx) pipeTo self
      context become connecting(Seq.empty)
  }

  private def connecting(requests: Seq[Promise[WorkerConnection]]): Receive = {
    case Event.AskConnection(req) =>
      context become connecting(req +: requests)

    case akka.actor.Status.Failure(e) =>
      log.error(e, s"Could not start worker connection for $id")
      context stop self

    case conn: WorkerConnection =>
      val wrapped = SharedConnector.ConnectionWrapper.wrap(conn)
      requests.foreach(_.success(wrapped))
      conn.whenTerminated.onComplete(_ => self ! Event.ConnTerminated)
      context become connected(wrapped)
  }

  private def connected(conn: WorkerConnection): Receive = {
    case Event.AskConnection(req) => req.success(conn)
    case Event.ConnTerminated =>
      log.info(s"Worker connection was terminated for $id")
      context stop self
  }
}

object SharedConnector {

  class ConnectionWrapper(target: ActorRef) extends Actor {

    override def preStart(): Unit = {
      context watch target
    }

    override def receive: Receive = {
      case ConnectionUnused => // ignore connection shutdown - only connector could stop it
      case Terminated(_) => context stop self
      case x => target forward x
    }
  }

  object ConnectionWrapper {
    def props(ref: ActorRef): Props = Props(classOf[ConnectionWrapper], ref)
    def wrap(connection: WorkerConnection)(implicit af: ActorRefFactory): WorkerConnection = {
      val wrappedRef = af.actorOf(props(connection.ref))
      connection.copy(ref = wrappedRef)
    }
  }


  def props(
    id: String,
    ctx: ContextConfig,
    startConnection: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(classOf[SharedConnector], id, ctx, startConnection)
}
