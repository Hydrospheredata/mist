package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import akka.pattern.pipe
import io.hydrosphere.mist.core.CommonData.{ConnectionUnused, ShutdownCommand}
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{Future, Promise}

class SharedConnector(
  id: String,
  ctx: ContextConfig,
  startConnection: () => Future[WorkerConnection]
) extends Actor with ActorLogging {

  import context.dispatcher

  override def receive: Receive = noConnection

  private def noConnection: Receive = {
    case Event.AskConnection(req) =>
      startConnection() pipeTo self
      context become process(Seq(req), Seq.empty, Map.empty, 1)

    case Event.WarnUp =>
      (0 to ctx.maxJobsOnNode)
        .foreach(_ => startConnection() pipeTo self)
      context become process(Seq.empty, Seq.empty, Map.empty, ctx.maxJobsOnNode)
  }

  private def process(
    requests: Seq[Promise[WorkerConnection]],
    pool: Seq[WorkerConnection],
    inUse: Map[String, WorkerConnection],
    startingConnections: Int
  ): Receive = {

    case conn: WorkerConnection if requests.isEmpty =>
      log.info(s"Receive $conn without requests, possibly warming up")
      val wrapped = SharedConnector.ConnectionWrapper.wrap(conn)
      conn.whenTerminated.onComplete(_ => self ! Event.ConnTerminated(conn.id))
      context become process(Seq.empty, pool :+ wrapped, inUse, startingConnections - 1)

    case conn: WorkerConnection =>
      log.info(s"Receive $conn trying to handle incoming request")
      val wrapped = SharedConnector.ConnectionWrapper.wrap(conn)
      conn.whenTerminated.onComplete(_ => self ! Event.ConnTerminated(conn.id))
      requests.head.success(wrapped)
      context become process(requests.tail, pool, inUse + (conn.id -> wrapped), startingConnections - 1)

    case akka.actor.Status.Failure(e) =>
      log.error(e, s"Could not start worker connection")
      context become process(requests, pool, inUse, startingConnections - 1)


    case Event.AskConnection(req) if pool.isEmpty && inUse.size + startingConnections < ctx.maxJobsOnNode =>
      log.info(s"Pool is empty and we are able to start new one connection: inUse size :${inUse.size}")
      startConnection() pipeTo self
      context become process(requests :+ req, pool, inUse, startingConnections + 1)

    case Event.AskConnection(req) if pool.nonEmpty =>
      log.info(s"Acquire connection from pool: pool size ${pool.size}")
      val updatedRequests = requests :+ req
      val available = Math.min(pool.size, requests.size)
      val toUsing = pool.take(available).zip(updatedRequests.take(available)).map {
        case (conn, pr) =>
          pr.success(conn)
          conn.id -> conn
      }.toMap
      context become process(
        updatedRequests.drop(available),
        pool.drop(available),
        inUse ++ toUsing,
        startingConnections
      )

    case Event.AskConnection(req) =>
      log.info(s"Schedule request: requests size ${requests.size}")
      context become process(requests :+ req, pool, inUse, startingConnections)

    case Event.ReleaseConnection(connectionId) =>
      log.info(s"Releasing connection: requested ${requests.size}, poooled ${pool.size}, in use ${inUse.size}, starting: ${startingConnections}")
      inUse.get(connectionId)
        .foreach(conn => requests.headOption match {
          case Some(req) =>
            req.success(conn)
            context become process(requests.tail, pool, inUse + (conn.id -> conn), startingConnections)
          case None =>
            context become process(Seq.empty, pool :+ conn, inUse - connectionId, startingConnections)
        })

    case cmd: ShutdownCommand =>
      requests.foreach(_.failure(new RuntimeException("connector was shutdown")))
      pool.foreach(_.ref ! cmd)
      inUse.values.foreach(_.ref ! cmd)
      if (startingConnections == 0) {
        context stop self
      } else {
        context become awaitingConnectionsAndShutdown(startingConnections)
      }

    case Event.ConnTerminated(connId) =>
      //keep
      SharedConnector.idGen.decrementAndGet()
      val updatedPool = pool.foldLeft(Seq.empty[WorkerConnection]) {
        case (acc, conn) if conn.id == connId => acc
        case (acc, conn) => acc :+ conn
      }
      context become process(requests, updatedPool, inUse - connId, startingConnections)
  }

  private def awaitingConnectionsAndShutdown(startingConnections: Int): Receive = {
    val lastConnection: Boolean = startingConnections == 1

    {
      case akka.actor.Status.Failure(e) if lastConnection =>
        log.error(e, "Could not start worker connection")
        context stop self
      case conn: WorkerConnection if lastConnection =>
        conn.shutdown(true)
        context stop self

      case akka.actor.Status.Failure(e) =>
        log.error(e, "Could not start worker connection")
        context become awaitingConnectionsAndShutdown(startingConnections - 1)

      case conn: WorkerConnection =>
        conn.shutdown(true)
        context become awaitingConnectionsAndShutdown(startingConnections - 1)
    }
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

  val idGen = new AtomicInteger(1)

  def props(
    id: String,
    ctx: ContextConfig,
    startConnection: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(classOf[SharedConnector], id, ctx, () => startConnection(s"$id-pool-${idGen.getAndIncrement()}", ctx))

}
