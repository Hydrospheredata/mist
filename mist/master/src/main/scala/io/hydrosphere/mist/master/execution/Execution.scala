package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef}
import io.hydrosphere.mist.core.CommonData.{CancelJobRequest, RunJobRequest}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

object Execution {

  sealed trait ExecutorEvent
  object ExecutorEvent {
    case class Started(ref: ActorRef) extends ExecutorEvent
    case class StartFailed(e: Throwable) extends ExecutorEvent
    case object Terminated extends ExecutorEvent
    case object Crushed extends ExecutorEvent
  }

  sealed trait ExecutorRestartStrategy
  case object DoNothing extends ExecutorRestartStrategy
  case class Retry(delay: Duration, limit: Option[Int]) extends ExecutorRestartStrategy


  implicit class FutureToMessage[A](future: Future[A])(implicit ac: ActorContext) {
    def pipeToSelfAs[B, C](f: A => B, err: Throwable => C): Unit = {
      future.onComplete({
        case Success(a) => ac.self ! f(a)
        case Failure(e) => ac.self ! err(e)
      })(ac.dispatcher)
    }
  }

  class QueueStage(
    initialState: ProcessingState,
    initialContext: ContextConfig,
    executorStarter: RemoteStarter,
    restartStrategy: ExecutorRestartStrategy
  ) extends Actor with ActorLogging {

    override def receive: Receive = waitRequests(initialState)

    private def waitRequests(state: ProcessingState): Receive = {
      case req: RunJobRequest =>
        val (next, st) = state.enqueue(req)
        startExecutor(initialContext)
        context.become(waitRequests(next))

    }

    private def waitExecutor(state: ProcessingState): Receive = {
      case req: RunJobRequest =>
        val (next, st) = state.enqueue(req)
        context.become(waitRequests(next))

      case CancelJobRequest(id) =>
        state.get(id) match {
          case Some(st) =>
            val (next, st) = state.remove(id)
            context.become(waitExecutor(next))

          case None =>
            //TODO
            log.warning("Handled wtf")
        }

      case ExecutorEvent.Started(ref) =>
        context.watchWith(ref, ExecutorEvent.Crushed)
        context.become(withExecutor(state, ref))

        state.next.foreach(st => ref ! st.request)

      case ExecutorEvent.StartFailed(err) =>

    }

    private def withExecutor(state: ProcessingState, ref: ActorRef): Receive = {
      case ExecutorEvent.Crushed =>

    }

    private def 

    private def startExecutor(context: ContextConfig): Unit = {
      executorStarter.start(context).pipeToSelfAs(
         ref => ExecutorEvent.Started(ref),
         e => ExecutorEvent.StartFailed(e)
      )
    }




  }
}
