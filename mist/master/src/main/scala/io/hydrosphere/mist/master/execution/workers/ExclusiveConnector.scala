package io.hydrosphere.mist.master.execution.workers

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import io.hydrosphere.mist.core.CommonData.{CompleteAndShutdown, RunJobRequest}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future
import scala.util._

class ExclusiveConnector(
  id: String,
  ctx: ContextConfig,
  startConnection: (String, ContextConfig) => Future[WorkerConnection]
) extends Actor with ActorLogging {

  import WorkerConnector._

  import context.dispatcher

  override def receive: Receive = process()

  var counter: Int = 1

  private def process(): Receive = {
    case Event.AskConnection(resolve) =>
      val wId = id + "_" + counter
      counter = counter + 1
      startConnection(wId, ctx).onComplete({
        case Success(worker) =>
          val wrapped = ExclusiveConnector.ConnectionWrapper.wrap(worker)
          resolve.success(wrapped)
        case Failure(e) => resolve.failure(e)
      })
  }
}

object ExclusiveConnector {

  // send completeAndShutdown after first run request
  class ConnectionWrapper(conn: ActorRef) extends Actor {

    override def preStart(): Unit = {
      context watch conn
    }

    override def receive: Receive = process(false)

    private def process(completeSent: Boolean): Receive = {
      case req: RunJobRequest =>
        conn forward req
        if (!completeSent) {
          conn ! CompleteAndShutdown
          context become process(true)
        }

      case Terminated(_) => context stop self
      case x => conn forward x
    }
  }

  object ConnectionWrapper {
    def props(conn: ActorRef): Props = Props(classOf[ConnectionWrapper], conn)
    def wrap(conn: WorkerConnection)(implicit fa: ActorRefFactory): WorkerConnection = {
      val actor = fa.actorOf(props(conn.ref))
      conn.copy(ref = actor)
    }
  }

  def props(
    id: String,
    ctx: ContextConfig,
    startWorker: (String, ContextConfig) => Future[WorkerConnection]
  ): Props = Props(classOf[ExclusiveConnector], id, ctx, startWorker)
}
