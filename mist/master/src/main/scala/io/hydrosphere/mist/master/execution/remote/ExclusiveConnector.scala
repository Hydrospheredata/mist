package io.hydrosphere.mist.master.execution.remote

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import io.hydrosphere.mist.core.CommonData.{CompeleteAndShutdown, RunJobRequest}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future
import scala.util._

class ExclusiveConnector(
  id: String,
  ctx: ContextConfig,
  startWorker: (String, ContextConfig) => Future[ActorRef]
) extends Actor with ActorLogging {

  import WorkerConnector._

  import context.dispatcher

  override def receive: Receive = process()

  var counter: Int = 1

  //TODO mutate state
  private def process(): Receive = {
    case Event.AskConnection(resolve) =>
      val wId = id + "_" + counter
      counter = counter + 1
      startWorker(wId, ctx).onComplete({
        case Success(worker) =>
          val wrapped = ExclusiveConnector.ConnectionWrapper.wrap(worker)
          resolve.success(WorkerConnection(wId, wrapped))
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

    private def process(compeleteSended: Boolean): Receive = {
      case req: RunJobRequest =>
        conn forward req
        if (!compeleteSended) {
          conn ! CompeleteAndShutdown
          context become process(true)
        }

      case Terminated(_) => context stop self
      case x => conn forward x
    }
  }

  object ConnectionWrapper {
    def props(conn: ActorRef): Props = Props(classOf[ConnectionWrapper], conn)
    def wrap(conn: ActorRef)(implicit fa: ActorRefFactory): ActorRef = {
      fa.actorOf(props(conn))
    }
  }

  def props(
    id: String,
    ctx: ContextConfig,
    startWorker: (String, ContextConfig) => Future[ActorRef]
  ): Props = Props(classOf[ExclusiveConnector], id, ctx, startWorker)
}
