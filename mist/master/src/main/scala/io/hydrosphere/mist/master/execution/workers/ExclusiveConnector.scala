package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future
import scala.util._

//TODO handle shurdown - memory leak
class ExclusiveConnector(
  id: String,
  ctx: ContextConfig,
  startConnection: (String, ContextConfig) => Future[WorkerConnection]
) extends Actor with ActorLogging {

  import WorkerConnector._
  import context.dispatcher

  type Conns = Map[String, WorkerConnection]

  override def receive: Receive = process(Map.empty)

  var counter: Int = 1

  private def process(conns: Conns): Receive = {
    case Event.AskConnection(resolve) =>
      val wId = id + "_" + counter
      counter = counter + 1
      startConnection(wId, ctx).onComplete({
        case Success(worker) =>
          val wrapped = ExclusiveConnector.wrappedConn(worker)
          resolve.success(wrapped)
        case Failure(e) => resolve.failure(e)
      })
    case Event.WarmUp =>
      log.warning("Exclusive connector {}: {} received warmup event", id, ctx.name)

//    // internal events for correctly handling shutdown event
//    case ExclusiveConnector.Event.Remember(conn) =>
//      context become process(conns + (conn.id -> conn))
//    case ExclusiveConnector.Event.Forget(connId) =>
//      context become process(conns - connId)
//
//    case Event.Shutdown(force) =>
//      conns.foreach({case (_, conn) => conn.shutdown(force)})
//      context stop self
  }
}

object ExclusiveConnector {

//  sealed trait Event
//  object Event {
//    final case class Remember(con: WorkerConnection) extends Event
//    final case class Forget(id: String) extends Event
//  }

  class ExclusivePerJobConnector(workerConn: WorkerConnection) extends PerJobConnection.Direct(workerConn) {
    import workerConn.ref
    override def run(req: RunJobRequest, respond: ActorRef): Unit = {
      ref.tell(req, respond)
      ref.tell(WorkerBridge.Event.CompleteAndShutdown, ActorRef.noSender)
    }

    override def cancel(id: String, respond: ActorRef): Unit = {
      ref.tell(CancelJobRequest(id), respond)
    }
    override def release(): Unit = ref.tell(WorkerBridge.Event.CompleteAndShutdown, ActorRef.noSender)
  }

  def wrappedConn(conn: WorkerConnection): PerJobConnection = new ExclusivePerJobConnector(conn)

  def props(
    id: String,
    ctx: ContextConfig,
    startWorker: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(classOf[ExclusiveConnector], id, ctx, startWorker)
}
