package io.hydrosphere.mist.master.execution.workers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import akka.pattern.pipe
import io.hydrosphere.mist.core.CommonData.{ReleaseConnection, ShutdownCommand}
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event
import io.hydrosphere.mist.master.models.ContextConfig

import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise}

class SharedConnector(
  id: String,
  ctx: ContextConfig,
  connectionStarter: String => Future[WorkerConnection],
  idGen: AtomicInteger = new AtomicInteger(1)
) extends Actor with ActorLogging {

  import context.dispatcher

  private def startConnection(): Future[WorkerConnection] = {
    val connectionId = s"$id-pool-${idGen.getAndIncrement()}"
    connectionStarter(connectionId)
  }

  override def receive: Receive = noConnection

  private def noConnection: Receive = {
    case Event.GetStatus => sender() ! SharedConnector.AwaitingRequest

    case Event.AskConnection(req) =>
      startConnection() pipeTo self
      context become process(Queue(req), Queue.empty, Map.empty, 1)

    case Event.WarmUp =>
      (0 until ctx.maxJobsOnNode).foreach(_ => startConnection() pipeTo self)
      context become process(Queue.empty, Queue.empty, Map.empty, ctx.maxJobsOnNode)
  }

  private def process(
    requests: Queue[Promise[WorkerConnection]],
    pool: Queue[WorkerConnection],
    inUse: Map[String, WorkerConnection],
    startingConnections: Int
  ): Receive = {

    case Event.GetStatus => sender() ! SharedConnector.ProcessStatus(requests.size, pool.size, inUse.size, startingConnections)

    case conn: WorkerConnection if requests.isEmpty =>
      log.info(s"Receive ${conn.id} without requests, possibly warming up")
      val wrapped = SharedConnector.ConnectionWrapper.wrap(self, conn)
      conn.whenTerminated.onComplete(_ => self ! Event.ConnTerminated(conn.id))
      context become process(Queue.empty, pool :+ wrapped, inUse, startingConnections - 1)

    case conn: WorkerConnection =>
      log.info(s"Receive ${conn.id} trying to handle incoming request")
      val wrapped = SharedConnector.ConnectionWrapper.wrap(self, conn)
      conn.whenTerminated.onComplete(_ => self ! Event.ConnTerminated(conn.id))
      val (req, updates) = requests.dequeue
      req.success(wrapped)
      context become process(updates, pool, inUse + (conn.id -> wrapped), startingConnections - 1)

    case akka.actor.Status.Failure(e) =>
      log.error(e, s"Could not start worker connection")
      //We need to handle such situations to keep context fronted state and shared connector state the same
      //in context frontend we decrement asked connections, but request is still alive
      //for example:
      //1. frontend.connector ! AskConn()
      //2. connector receive AskConn
      //3. connector start new connection for extending pool
      //4. connection failed -> context frontend do not know anything about it
      //5. context frontend ask connection repeatedly, but fails to get it
      //6. request queue become large, all machine resources is targeted to start connection
      requests.dequeueOption match {
        case Some((req, newRequests)) =>
          req.failure(e)
          context become process(newRequests, pool, inUse, startingConnections - 1)
        case None =>
          log.warning("Failed to initialize connection")
          context become process(requests, pool, inUse, startingConnections - 1)
      }

    case Event.AskConnection(req) if pool.isEmpty && inUse.size + startingConnections < ctx.maxJobsOnNode =>
      log.info(s"Pool is empty and we are able to start new one connection: inUse size :${inUse.size}")
      startConnection() pipeTo self
      context become process(requests :+ req, pool, inUse, startingConnections + 1)

    case Event.AskConnection(req) if pool.nonEmpty =>
      log.info(s"Acquire connection from pool: pool size ${pool.size}, requests size: ${requests.size + 1}")
      val updatedRequests = requests :+ req
      val (nextReq, newRequests) = updatedRequests.dequeue
      val (conn, newPool) = pool.dequeue
      nextReq.success(conn)
      context become process(newRequests, newPool, inUse + (conn.id -> conn), startingConnections)

    case Event.AskConnection(req) =>
      log.info(s"Schedule request: requests size ${requests.size}")
      context become process(requests :+ req, pool, inUse, startingConnections)

    case ReleaseConnection(connId) =>
      log.info(s"Releasing connection: requested ${requests.size}, pooled ${pool.size}, in use ${inUse.size}, starting: $startingConnections")
      inUse.get(connId) match {
        case Some(releasedConnection) =>
          val withReleasedConnection = pool :+ releasedConnection
          val removedUsedConnection = inUse - connId
          requests.dequeueOption match {
            case Some((req, rest)) =>
              val (conn, newPool) = withReleasedConnection.dequeue
              req.success(conn)
              context become process(rest, newPool, removedUsedConnection + (conn.id -> conn), startingConnections)
            case None =>
              context become process(Queue.empty, withReleasedConnection, removedUsedConnection, startingConnections)
          }
        case None =>
          log.info("Released unused connection")
      }

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
      idGen.decrementAndGet()
      val updatedPool = pool.filterNot(_.id == connId)
      context become process(requests, updatedPool, inUse - connId, startingConnections)
  }

  private def awaitingConnectionsAndShutdown(startingConnections: Int): Receive = {
    val lastConnection: Boolean = startingConnections == 1

    {
      case Event.GetStatus => sender() ! SharedConnector.ShuttingDown(startingConnections)

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

  sealed trait Status
  case object AwaitingRequest extends Status
  case class ProcessStatus(requestsSize: Int, poolSize: Int, inUseSize: Int, startingConnections: Int) extends Status
  case class ShuttingDown(startingConnections: Int) extends Status

  class ConnectionWrapper(connector: ActorRef, target: ActorRef) extends Actor {

    override def preStart(): Unit = {
      context watch target
    }

    override def receive: Receive = {
      case ReleaseConnection(id) =>
        connector ! ReleaseConnection(id)
      case Terminated(_) => context stop self
      case x => target forward x
    }
  }

  object ConnectionWrapper {
    def props(connector: ActorRef, ref: ActorRef): Props = Props(classOf[ConnectionWrapper], connector, ref)

    def wrap(connector: ActorRef, connection: WorkerConnection)(implicit af: ActorRefFactory): WorkerConnection = {
      val wrappedRef = af.actorOf(props(connector, connection.ref))
      connection.copy(ref = wrappedRef)
    }
  }

  def props(
    id: String,
    ctx: ContextConfig,
    startConnection: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(new SharedConnector(id, ctx, (id: String) => startConnection(id, ctx)))

}
