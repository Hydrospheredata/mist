package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import io.hydrosphere.mist.core.CommonData
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.execution.workers.WorkerConnector.Event.Released
import io.hydrosphere.mist.master.models.ContextConfig

import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise}
import scala.util._

class ExclusiveConnector(
  id: String,
  ctx: ContextConfig,
  startConnection: (String, ContextConfig) => Future[WorkerConnection]
) extends Actor with ActorLogging {

  import WorkerConnector._
  import context.dispatcher

  type Conns = Map[String, WorkerConnection]

  override def receive: Receive = process(Queue.empty, Map.empty, 0)

  var counter: Int = 1

  private def nextId(): String = {
    val wId = id + "_" + counter
    counter = counter + 1
    wId
  }

  private def process(
    requests: Queue[Promise[PerJobConnection]],
    working: Conns,
    startingConnections: Int
  ): Receive = {
    case Event.AskConnection(req) =>
      startConnection(nextId(), ctx) pipeTo self
      context become process(requests :+ req, working, startingConnections + 1)

    case conn: WorkerConnection =>
      val (req, other) = requests.dequeue
      req.success(ExclusiveConnector.wrappedConnection(self, conn))
      context become process(other, working + (conn.id -> conn), startingConnections - 1)

    case akka.actor.Status.Failure(e) =>
      log.error(e, "Could not start worker connection")
      val (req, other) = requests.dequeue
      req.failure(e)
      context become process(other, working, startingConnections - 1)

    case Event.WarmUp =>
      log.warning("Exclusive connector {}: {} received warmup event", id, ctx.name)

    case Event.Released(conn) =>
      conn.shutdown(true)
      context become process(requests, working - conn.id, startingConnections)

    case Event.Shutdown(force) =>
      requests.foreach(_.failure(new RuntimeException("connector was shutdown")))
      working.foreach({case (_, conn) => conn.shutdown(force)})
      if (startingConnections > 0) {
        context become awaitingConnectionsAndShutdown(startingConnections, force)
      } else {
        context stop self
      }
  }

  private def awaitingConnectionsAndShutdown(startingConnections: Int, force: Boolean): Receive = {
    val lastConnection: Boolean = startingConnections == 1

    {
      case akka.actor.Status.Failure(e) if lastConnection =>
        log.error(e, "Could not start worker connection")
        context stop self

      case conn: WorkerConnection if lastConnection =>
        conn.shutdown(force)
        context stop self

      case akka.actor.Status.Failure(e) =>
        log.error(e, "Could not start worker connection")
        context become awaitingConnectionsAndShutdown(startingConnections - 1, force)

      case conn: WorkerConnection =>
        conn.shutdown(force)
        context become awaitingConnectionsAndShutdown(startingConnections - 1, force)
    }
  }
}

object ExclusiveConnector {

  class ExclusivePerJobConnector(
    connector: ActorRef,
    direct: WorkerConnection
  ) extends PerJobConnection.Direct(direct) {

    import direct.ref
    def run(req: CommonData.RunJobRequest, respond: ActorRef): Unit = {
      ref.tell(req, respond)
      ref.tell(WorkerBridge.Event.CompleteAndShutdown, ActorRef.noSender)
    }
    def cancel(id: String, respond: ActorRef): Unit = ref.tell(CancelJobRequest(id), respond)
    def release(): Unit = connector ! Released(direct)
  }

  def wrappedConnection(connector: ActorRef, conn: WorkerConnection): PerJobConnection =
    new ExclusivePerJobConnector(connector, conn)

  def props(
    id: String,
    ctx: ContextConfig,
    startWorker: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(classOf[ExclusiveConnector], id, ctx, startWorker)
}
